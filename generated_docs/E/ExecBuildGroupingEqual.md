# ExecBuildGroupingEqual

## Location
[src/backend/executor/execExpr.c:3957-4113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L3957-L4113)

## Overview
Builds an equality expression that can be evaluated using ExecQual(), returning true if the expression context's inner/outer tuples are NOT DISTINCT (i.e., two nulls match, but a null and a non-null don't match).

## Definition

```c
ExprState *
ExecBuildGroupingEqual(TupleDesc ldesc, TupleDesc rdesc,
					   const TupleTableSlotOps *lops, const TupleTableSlotOps *rops,
					   int numCols,
					   const AttrNumber *keyColIdx,
					   const Oid *eqfunctions,
					   const Oid *collations,
					   PlanState *parent)
```
## Detailed Description
ExecBuildGroupingEqual constructs a specialized expression evaluation state for comparing tuples with NOT DISTINCT semantics. Unlike regular equality comparisons, this function treats NULL values specially - two NULL values are considered equal, which is essential for grouping operations where NULL values should be grouped together.

The function builds a series of evaluation steps that:
- Deforms both left and right tuples to access the required attributes
- Compares attributes in reverse order (starting from the last field) for optimization with sorted input
- Uses NOT DISTINCT comparison semantics for each attribute pair
- Short-circuits on the first non-matching attribute using QUAL steps
- Handles proper permissions checking for equality functions
- Returns NULL for zero-column comparisons (always true case)

## Parameters / Member Variables
- `ldesc`: TupleDesc describing the structure of left (inner) tuples to compare
- `rdesc`: TupleDesc describing the structure of right (outer) tuples to compare
- `*lops`: TupleTableSlotOps for left tuple operations
- `*rops`: TupleTableSlotOps for right tuple operations
- `numCols`: Integer specifying the number of attributes to examine in the comparison
- `*keyColIdx`: Array of AttrNumber values indicating which column indices to compare
- `*eqfunctions`: Array of Oid values specifying the equality function OIDs to use for each attribute
- `*collations`: Array of Oid values specifying the collation OIDs for each attribute
- `*parent`: PlanState pointer to the parent executor node
## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - TupleDescAttr
  - [ExecComputeSlotInfo](ExecComputeSlotInfo.md)
  - [ExprEvalPushStep](ExprEvalPushStep.md)  
  - [object_aclcheck](../o/object_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_func_name](../g/get_func_name.md)
  - InvokeFunctionExecuteHook
  - [fmgr_info](../f/fmgr_info.md)
  - fmgr_info_set_expr
  - InitFunctionCallInfoData
  - SizeForFunctionCallInfo
  - [lappend_int](../l/lappend_int.md)
  - lfirst_int
  - [ExecReadyExpr](ExecReadyExpr.md)
  - EEOP_INNER_FETCHSOME
  - EEOP_OUTER_FETCHSOME
  - EEOP_INNER_VAR
  - EEOP_OUTER_VAR
  - EEOP_NOT_DISTINCT
  - EEOP_QUAL
  - EEOP_DONE
- Called from (representative examples):
  - [execTuplesMatchPrepare](../e/execTuplesMatchPrepare.md)
  - [BuildTupleHashTableExt](../B/BuildTupleHashTableExt.md)
  - [ExecInitSubPlan](ExecInitSubPlan.md)

## Notes and Other Information
- Located in src/backend/executor/execExpr.c (lines 3957-4113)
- Implements NOT DISTINCT semantics where NULL = NULL is true
- Optimizes comparison order by starting with the last field (most significant for sorted input)
- Performs security checks by validating function execution permissions
- Returns NULL when numCols is 0, indicating all comparisons should return true
- Uses short-circuit evaluation via QUAL opcodes to exit early on mismatches
- Essential for proper GROUP BY behavior with NULL values in PostgreSQL

## Simplified Source

```c
ExprState *ExecBuildGroupingEqual(TupleDesc ldesc, TupleDesc rdesc,
                                const TupleTableSlotOps *lops, const TupleTableSlotOps *rops,
                                int numCols, const AttrNumber *keyColIdx,
                                const Oid *eqfunctions, const Oid *collations,
                                PlanState *parent) {
    ExprState *state = makeNode(ExprState);

    // Handle zero-column comparison case
    if (numCols == 0)
        return NULL;

    // Initialize expression state
    state->expr = NULL;
    state->flags = EEO_FLAG_IS_QUAL;
    state->parent = parent;

    // Find maximum attribute number for slot deformation
    int maxatt = -1;
    for (int natt = 0; natt < numCols; natt++) {
        int attno = keyColIdx[natt];
        if (attno > maxatt)
            maxatt = attno;
    }

    // Push deformation steps for both inner and outer tuples
    ExprEvalStep scratch = {0};
    scratch.opcode = EEOP_INNER_FETCHSOME;
    scratch.d.fetch.last_var = maxatt;
    scratch.d.fetch.known_desc = ldesc;
    scratch.d.fetch.kind = lops;
    if (ExecComputeSlotInfo(state, &scratch))
        ExprEvalPushStep(state, &scratch);

    scratch.opcode = EEOP_OUTER_FETCHSOME;
    scratch.d.fetch.known_desc = rdesc;
    scratch.d.fetch.kind = rops;
    if (ExecComputeSlotInfo(state, &scratch))
        ExprEvalPushStep(state, &scratch);

    // Compare attributes in reverse order for optimization
    List *adjust_jumps = NIL;
    for (int natt = numCols; --natt >= 0;) {
        int attno = keyColIdx[natt];
        Oid foid = eqfunctions[natt];
        Oid collid = collations[natt];

        // Setup function call info with permission checking
        FmgrInfo *finfo = palloc0(sizeof(FmgrInfo));
        FunctionCallInfo fcinfo = palloc0(SizeForFunctionCallInfo(2));
        fmgr_info(foid, finfo);
        InitFunctionCallInfoData(*fcinfo, finfo, 2, collid, NULL, NULL);

        // Generate steps: left var, right var, NOT DISTINCT, QUAL
        // Left argument
        scratch.opcode = EEOP_INNER_VAR;
        scratch.d.var.attnum = attno - 1;
        scratch.resvalue = &fcinfo->args[0].value;
        scratch.resnull = &fcinfo->args[0].isnull;
        ExprEvalPushStep(state, &scratch);

        // Right argument
        scratch.opcode = EEOP_OUTER_VAR;
        scratch.d.var.attnum = attno - 1;
        scratch.resvalue = &fcinfo->args[1].value;
        scratch.resnull = &fcinfo->args[1].isnull;
        ExprEvalPushStep(state, &scratch);

        // NOT DISTINCT evaluation
        scratch.opcode = EEOP_NOT_DISTINCT;
        scratch.d.func.finfo = finfo;
        scratch.d.func.fcinfo_data = fcinfo;
        scratch.resvalue = &state->resvalue;
        scratch.resnull = &state->resnull;
        ExprEvalPushStep(state, &scratch);

        // QUAL step for short-circuit evaluation
        scratch.opcode = EEOP_QUAL;
        scratch.d.qualexpr.jumpdone = -1;  // Adjusted later
        ExprEvalPushStep(state, &scratch);
        adjust_jumps = lappend_int(adjust_jumps, state->steps_len - 1);
    }

    // Adjust jump targets and finalize
    foreach(lc, adjust_jumps) {
        ExprEvalStep *as = &state->steps[lfirst_int(lc)];
        as->d.qualexpr.jumpdone = state->steps_len;
    }

    scratch.opcode = EEOP_DONE;
    ExprEvalPushStep(state, &scratch);
    ExecReadyExpr(state);

    return state;
}
```