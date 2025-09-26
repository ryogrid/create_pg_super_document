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
- : TupleDesc describing the structure of tuples to be compared
- : TupleTableSlotOps for left (inner) tuple operations
- : TupleTableSlotOps for right (outer) tuple operations
- : Array of Oid values specifying equality function OIDs, must match length of param_exprs list
- : Array of Oid values specifying collation OIDs for equality comparison, must match length of param_exprs list
- : List of parameter expressions defining the comparison parameters
- : PlanState pointer to the parent executor node

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