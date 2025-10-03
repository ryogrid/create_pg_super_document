# ExecBuildParamSetEqual

## Location
[src/backend/executor/execExpr.c:4114-4235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L4114-L4235)

## Overview
Builds an equality expression that can be evaluated using ExecQual(), returning true if the expression context's inner/outer tuples are equal, where datums are assumed to be in the same order and quantity as the equality functions parameter, and NULLs are treated as equal.

## Definition

```c
ExprState *
ExecBuildParamSetEqual(TupleDesc desc,
					   const TupleTableSlotOps *lops,
					   const TupleTableSlotOps *rops,
					   const Oid *eqfunctions,
					   const Oid *collations,
					   const List *param_exprs,
					   PlanState *parent)
```
## Detailed Description
ExecBuildParamSetEqual constructs a specialized expression evaluation state for comparing tuples where the comparison parameters are explicitly defined through a parameter expression list. This function is similar to ExecBuildGroupingEqual but is designed for scenarios where the comparison is based on a predetermined set of parameters rather than arbitrary column indices.

The function builds evaluation steps that:
- Deforms both left and right tuples to access all required attributes up to the maximum parameter count
- Iterates through each attribute position in sequential order (unlike the reverse order in ExecBuildGroupingEqual)
- Uses NOT DISTINCT comparison semantics, treating NULL values as equal
- Performs permission checking for each equality function
- Uses short-circuit evaluation with QUAL steps to exit on first mismatch
- Assumes datums in inner/outer slots are in the same order as the equality functions

## Parameters / Member Variables
- `desc`: TupleDesc describing the structure of tuples to be compared
- `*lops`: TupleTableSlotOps for left (inner) tuple operations
- `*rops`: TupleTableSlotOps for right (outer) tuple operations
- `*eqfunctions`: Array of Oid values specifying equality function OIDs, must match length of param_exprs list
- `*collations`: Array of Oid values specifying collation OIDs for equality comparison, must match length of param_exprs list
- `*param_exprs`: List of parameter expressions defining the comparison parameters
- `*parent`: PlanState pointer to the parent executor node
## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [list_length](../l/list_length.md)
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
  - [ExecInitMemoize](ExecInitMemoize.md)

## Notes and Other Information
- Located in src/backend/executor/execExpr.c (lines 4114-4235)
- Implements NOT DISTINCT semantics where NULL = NULL is true
- Differs from ExecBuildGroupingEqual by processing attributes in sequential order rather than reverse order
- Uses the length of param_exprs list to determine the maximum attribute number to fetch
- Assumes a direct correspondence between parameter expressions and equality functions
- Essential for memoization operations where parameter sets need to be compared for cache hits
- Performs security validation by checking function execution permissions for each equality function

## Simplified Source

```c
ExprState *
ExecBuildParamSetEqual(TupleDesc desc,
                       const TupleTableSlotOps *lops,
                       const TupleTableSlotOps *rops,
                       const Oid *eqfunctions,
                       const Oid *collations,
                       const List *param_exprs,
                       PlanState *parent)
{
    // Initialize expression state for tuple equality comparison
    ExprState *state = makeNode(ExprState);
    ExprEvalStep step = {0};
    int max_attrs = list_length(param_exprs);
    List *jump_fixups = NIL;

    // Set up basic state properties
    state->expr = NULL;
    state->flags = EEO_FLAG_IS_QUAL;
    state->parent = parent;
    step.resvalue = &state->resvalue;
    step.resnull = &state->resnull;

    // Add steps to fetch all needed attributes from inner tuple
    step.opcode = EEOP_INNER_FETCHSOME;
    step.d.fetch.last_var = max_attrs;
    step.d.fetch.known_desc = desc;
    step.d.fetch.kind = lops;
    if (ExecComputeSlotInfo(state, &step))
        ExprEvalPushStep(state, &step);

    // Add steps to fetch all needed attributes from outer tuple
    step.opcode = EEOP_OUTER_FETCHSOME;
    step.d.fetch.last_var = max_attrs;
    step.d.fetch.known_desc = desc;
    step.d.fetch.kind = rops;
    if (ExecComputeSlotInfo(state, &step))
        ExprEvalPushStep(state, &step);

    // For each attribute, build comparison steps
    for (int attno = 0; attno < max_attrs; attno++)
    {
        Form_pg_attribute attr = TupleDescAttr(desc, attno);
        Oid eq_func_oid = eqfunctions[attno];
        Oid collation_oid = collations[attno];

        // Check permission to execute equality function
        check_function_execute_permission(eq_func_oid);

        // Set up function call info for equality comparison
        FmgrInfo *finfo = setup_equality_function(eq_func_oid);
        FunctionCallInfo fcinfo = setup_function_call_info(finfo, collation_oid);

        // Add step to get left operand from inner tuple
        add_var_fetch_step(state, EEOP_INNER_VAR, attno, attr->atttypid,
                          &fcinfo->args[0]);

        // Add step to get right operand from outer tuple
        add_var_fetch_step(state, EEOP_OUTER_VAR, attno, attr->atttypid,
                          &fcinfo->args[1]);

        // Add step to perform NOT DISTINCT comparison (NULL = NULL is true)
        add_not_distinct_step(state, finfo, fcinfo);

        // Add qualifier step that exits early if comparison is false
        add_qual_step(state, &jump_fixups);
    }

    // Fix up jump targets to point to end of expression
    fix_jump_targets(state, jump_fixups);

    // Add final DONE step
    step.opcode = EEOP_DONE;
    ExprEvalPushStep(state, &step);

    ExecReadyExpr(state);
    return state;
}
```