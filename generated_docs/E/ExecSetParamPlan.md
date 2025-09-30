# ExecSetParamPlan

## Location
[src/backend/executor/nodeSubplan.c:1092-1267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSubplan.c#L1092-L1267)

## Overview
Executes a subplan and sets its output parameters, implementing lazy evaluation of initplans by running the subplan only when parameter values are actually needed.

## Definition
```c
void ExecSetParamPlan(SubPlanState *node, ExprContext *econtext)
```

## Detailed Description
ExecSetParamPlan is the core function for executing initplans and setting their output parameters. It implements lazy evaluation by only running subplans when their PARAM_EXEC parameter values are requested through ExecEvalParamExec(). The function handles different types of sublinks (EXISTS, ARRAY, EXPR, MULTIEXPR, ROWCOMPARE) with appropriate semantics for each.

Key behaviors include:
1. Enforces forward scan direction regardless of caller context
2. For EXISTS sublinks, sets a boolean parameter based on whether any rows are found
3. For ARRAY sublinks, collects all result values into an array using ArrayBuildStateAny
4. For expression sublinks, ensures exactly one row is returned and copies tuple data
5. Properly manages memory by switching to per-query context for result storage
6. Clears execPlan fields after evaluation to prevent re-execution

The function includes comprehensive error checking for unsupported sublink types (ANY/ALL, CTE) and correlated subplans. It carefully manages memory allocation by copying subplan tuples and array results into the query's memory context to ensure data persistence beyond the function call.

## Parameters / Member Variables
- `node`: The SubPlanState containing the subplan to execute and state information
- `econtext`: The ExprContext providing parameter storage and memory context for evaluation

## Dependencies
- Functions called/Symbols referenced:
  - [ExecProcNode](ExecProcNode.md) (to execute the subplan)
  - TupIsNull (to check for end of results)
  - [initArrayResultAny](../i/initArrayResultAny.md), accumArrayResultAny, makeArrayResultAny (for array handling)
  - [ExecCopySlotHeapTuple](ExecCopySlotHeapTuple.md) (to copy tuple data)
  - [heap_getattr](../h/heap_getattr.md), heap_freetuple (for heap tuple operations)
  - [slot_getattr](../s/slot_getattr.md) (for slot attribute access)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (for memory context management)
  - linitial_int, lfirst_int (for list operations)
- Types used:
  - [SubPlanState](../S/SubPlanState.md), SubPlan, PlanState, EState
  - [SubLinkType](../S/SubLinkType.md), ScanDirection, ExprContext
  - [ParamExecData](../P/ParamExecData.md), ArrayBuildStateAny
  - [TupleTableSlot](../T/TupleTableSlot.md), TupleDesc
- Called from (representative examples):
  - [ExecEvalParamExec](ExecEvalParamExec.md) (when parameter value is needed)
  - [ExecSetParamPlanMulti](ExecSetParamPlanMulti.md) (for multi-parameter execution)

## Notes and Other Information
- This function MUST clear execPlan fields after evaluating parameters to prevent re-execution
- Results are stored in the EState's ecxt_param_exec_vals array, with pass-by-ref datums allocated in per-query memory
- Enforces cardinality constraints for expression sublinks (exactly one row expected)
- Uses forward scan direction internally but restores original direction before returning
- For ARRAY sublinks, manages memory carefully to avoid leaks across repeated calls
- The function assumes non-correlated subplans (parParam should be NIL)
- Handles NULL results appropriately for different sublink types (false for EXISTS, NULL for others)
- Memory context switching ensures results persist after the function returns while using caller's context for temporary operations

## Simplified Source

```c
void ExecSetParamPlan(SubPlanState *node, ExprContext *econtext) {
    SubPlan *subplan = node->subplan;
    PlanState *planstate = node->planstate;
    SubLinkType subLinkType = subplan->subLinkType;
    EState *estate = planstate->state;
    ScanDirection dir = estate->es_direction;
    MemoryContext oldcontext;
    TupleTableSlot *slot;
    bool found = false;
    ArrayBuildStateAny *astate = NULL;

    // Error checking for unsupported sublink types
    if (subLinkType == ANY_SUBLINK || subLinkType == ALL_SUBLINK)
        elog(ERROR, "ANY/ALL subselect unsupported as initplan");
    if (subLinkType == CTE_SUBLINK)
        elog(ERROR, "CTE subplans should not be executed via ExecSetParamPlan");
    if (subplan->parParam || node->args)
        elog(ERROR, "correlated subplans should not be executed via ExecSetParamPlan");

    // Enforce forward scan direction
    estate->es_direction = ForwardScanDirection;

    // Initialize array builder for ARRAY sublinks
    if (subLinkType == ARRAY_SUBLINK)
        astate = initArrayResultAny(subplan->firstColType, CurrentMemoryContext, true);

    // Switch to per-query memory context for results
    oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

    // Execute the subplan and process results
    for (slot = ExecProcNode(planstate); !TupIsNull(slot); slot = ExecProcNode(planstate)) {
        TupleDesc tdesc = slot->tts_tupleDescriptor;
        int i = 1;

        if (subLinkType == EXISTS_SUBLINK) {
            // For EXISTS, just set parameter to true and exit
            int paramid = linitial_int(subplan->setParam);
            ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

            prm->execPlan = NULL;
            prm->value = BoolGetDatum(true);
            prm->isnull = false;
            found = true;
            break;
        }

        if (subLinkType == ARRAY_SUBLINK) {
            // Collect all values into array
            Datum dvalue;
            bool disnull;

            found = true;
            Assert(subplan->firstColType == TupleDescAttr(tdesc, 0)->atttypid);
            dvalue = slot_getattr(slot, 1, &disnull);
            astate = accumArrayResultAny(astate, dvalue, disnull,
                                         subplan->firstColType, oldcontext);
            continue;
        }

        // Check cardinality for expression sublinks
        if (found && (subLinkType == EXPR_SUBLINK || subLinkType == MULTIEXPR_SUBLINK ||
                      subLinkType == ROWCOMPARE_SUBLINK))
            ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
                    errmsg("more than one row returned by a subquery used as an expression")));

        found = true;

        // Copy tuple and set parameters for expression sublinks
        if (node->curTuple)
            heap_freetuple(node->curTuple);
        node->curTuple = ExecCopySlotHeapTuple(slot);

        // Set all output parameters from tuple columns
        foreach(ListCell, l, subplan->setParam) {
            int paramid = lfirst_int(l);
            ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

            prm->execPlan = NULL;
            prm->value = heap_getattr(node->curTuple, i, tdesc, &(prm->isnull));
            i++;
        }
    }

    // Handle final result setting
    if (subLinkType == ARRAY_SUBLINK) {
        // Build final array result
        int paramid = linitial_int(subplan->setParam);
        ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

        if (node->curArray != PointerGetDatum(NULL))
            pfree(DatumGetPointer(node->curArray));
        node->curArray = makeArrayResultAny(astate, econtext->ecxt_per_query_memory, true);
        prm->execPlan = NULL;
        prm->value = node->curArray;
        prm->isnull = false;
    }
    else if (!found) {
        // Handle no results case
        if (subLinkType == EXISTS_SUBLINK) {
            int paramid = linitial_int(subplan->setParam);
            ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

            prm->execPlan = NULL;
            prm->value = BoolGetDatum(false);
            prm->isnull = false;
        }
        else {
            // Set all parameters to NULL
            foreach(ListCell, l, subplan->setParam) {
                int paramid = lfirst_int(l);
                ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

                prm->execPlan = NULL;
                prm->value = (Datum) 0;
                prm->isnull = true;
            }
        }
    }

    // Restore contexts and scan direction
    MemoryContextSwitchTo(oldcontext);
    estate->es_direction = dir;
}
```