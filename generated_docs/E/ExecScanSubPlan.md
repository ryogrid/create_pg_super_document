# ExecScanSubPlan

## Location
[src/backend/executor/nodeSubplan.c:223-503](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSubplan.c#L223-L503)

## Overview
ExecScanSubPlan executes subplans by scanning through subquery results tuple-by-tuple, handling all sublink types (EXISTS, ANY, ALL, EXPR, MULTIEXPR, ARRAY, ROWCOMPARE) with appropriate SQL semantics.

## Definition
```c
static Datum ExecScanSubPlan(SubPlanState *node, ExprContext *econtext, bool *isNull)
```

## Detailed Description
ExecScanSubPlan implements the default subplan execution strategy that rescans the subplan for each evaluation. Unlike hash-based execution, this approach processes subquery results sequentially, making it suitable for correlated subqueries and cases where hash tables are not beneficial.

The function handles all PostgreSQL sublink types with distinct semantics:
- EXISTS: Returns TRUE if any tuple is found
- EXPR: Returns the first column of a single tuple (error if multiple)
- MULTIEXPR: Distributes columns of a single tuple to multiple output parameters
- ARRAY: Collects first column values from all tuples into an array
- ANY: Combines results using OR semantics across tuples
- ALL: Combines results using AND semantics across tuples
- ROWCOMPARE: Compares a single tuple against left-hand values

The function properly handles parameter passing for correlated subqueries, memory management for pass-by-reference types, and SQL's three-valued logic for NULL handling.

## Parameters / Member Variables
- `node`: SubPlanState containing execution state, parameter mappings, and test expressions
- `econtext`: ExprContext providing evaluation context and parameter values for correlation
- `isNull`: Pointer to boolean flag indicating NULL result according to SQL three-valued logic

## Dependencies
- Functions called/Symbols referenced:
  - [initArrayResultAny](../i/initArrayResultAny.md)/accumArrayResultAny/makeArrayResultAny (array building for ARRAY_SUBLINK)
  - [ExecEvalExprSwitchContext](ExecEvalExprSwitchContext.md) (parameter evaluation with context switching)
  - [ExecReScan](ExecReScan.md) (resetting subplan for re-execution)
  - [ExecProcNode](ExecProcNode.md) (fetching tuples from subplan)
  - [ExecCopySlotHeapTuple](ExecCopySlotHeapTuple.md) (copying tuples for pass-by-ref data)
  - [heap_getattr](../h/heap_getattr.md) (extracting column values from tuples)
  - [heap_freetuple](../h/heap_freetuple.md) (freeing copied tuples)
  - [slot_getattr](../s/slot_getattr.md) (extracting values from tuple slots)
  - [ParamExecData](../P/ParamExecData.md) (parameter execution data structure)
- Called from (representative examples):
  - [ExecSubPlan](ExecSubPlan.md) (in nodeSubplan.c:89)

## Notes and Other Information
- Switches to per-query memory context for subplan execution to ensure proper memory management
- Supports correlated subqueries through parameter passing (parParam/paramIds)
- Enforces single-tuple constraints for EXPR, MULTIEXPR, and ROWCOMPARE sublinks
- Implements proper SQL semantics for empty results: FALSE for ANY, TRUE for ALL, NULL for EXPR/ROWCOMPARE
- Manages tuple copying for pass-by-reference types to prevent dangling pointers
- Uses forboth() macro for synchronized iteration over parameter lists
- Handles all sublink types in a single unified function with branching logic
- Properly restores memory context after execution
- For MULTIEXPR_SUBLINK, the return value is dummy (false) as real results go to setParam parameters

## Simplified Source

```c
static Datum ExecScanSubPlan(SubPlanState *node, ExprContext *econtext, bool *isNull) {
    SubLinkType subLinkType = node->subplan->subLinkType;
    MemoryContext oldcontext;
    bool found = false;
    ArrayBuildStateAny *astate = NULL;

    // Initialize array builder for ARRAY_SUBLINK
    if (subLinkType == ARRAY_SUBLINK)
        astate = initArrayResultAny(node->subplan->firstColType, CurrentMemoryContext, true);

    // Switch to per-query context for subplan execution
    oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

    // Set correlation parameters from parent plan
    forboth(l, node->subplan->parParam, pvar, node->args) {
        int paramid = lfirst_int(l);
        ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);
        prm->value = ExecEvalExprSwitchContext((ExprState *) lfirst(pvar), econtext, &(prm->isnull));
        node->planstate->chgParam = bms_add_member(node->planstate->chgParam, paramid);
    }

    // Reset and scan subplan
    ExecReScan(node->planstate);
    Datum result = BoolGetDatum(subLinkType == ALL_SUBLINK);
    *isNull = false;

    for (TupleTableSlot *slot = ExecProcNode(node->planstate);
         !TupIsNull(slot);
         slot = ExecProcNode(node->planstate)) {

        if (subLinkType == EXISTS_SUBLINK) {
            result = BoolGetDatum(true);
            break;
        }
        else if (subLinkType == EXPR_SUBLINK) {
            if (found)
                ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
                               errmsg("more than one row returned by a subquery used as an expression")));
            found = true;
            if (node->curTuple) heap_freetuple(node->curTuple);
            node->curTuple = ExecCopySlotHeapTuple(slot);
            result = heap_getattr(node->curTuple, 1, slot->tts_tupleDescriptor, isNull);
            continue;
        }
        else if (subLinkType == ARRAY_SUBLINK) {
            found = true;
            Datum dvalue = slot_getattr(slot, 1, &disnull);
            astate = accumArrayResultAny(astate, dvalue, disnull, node->subplan->firstColType, oldcontext);
            continue;
        }
        else if (subLinkType == ANY_SUBLINK || subLinkType == ALL_SUBLINK) {
            // Load parameters and evaluate test expression
            int col = 1;
            foreach(plst, node->subplan->paramIds) {
                int paramid = lfirst_int(plst);
                ParamExecData *prmdata = &(econtext->ecxt_param_exec_vals[paramid]);
                prmdata->value = slot_getattr(slot, col++, &(prmdata->isnull));
            }

            Datum rowresult = ExecEvalExprSwitchContext(node->testexpr, econtext, &rownull);

            if (subLinkType == ANY_SUBLINK) {
                if (rownull) *isNull = true;
                else if (DatumGetBool(rowresult)) {
                    result = BoolGetDatum(true);
                    *isNull = false;
                    break;
                }
            } else { // ALL_SUBLINK
                if (rownull) *isNull = true;
                else if (!DatumGetBool(rowresult)) {
                    result = BoolGetDatum(false);
                    *isNull = false;
                    break;
                }
            }
        }
        found = true;
    }

    MemoryContextSwitchTo(oldcontext);

    // Handle result based on sublink type
    if (subLinkType == ARRAY_SUBLINK) {
        result = makeArrayResultAny(astate, oldcontext, true);
    } else if (!found && (subLinkType == EXPR_SUBLINK || subLinkType == ROWCOMPARE_SUBLINK)) {
        result = (Datum) 0;
        *isNull = true;
    }

    return result;
}
```