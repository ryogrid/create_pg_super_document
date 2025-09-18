# ExecBuildGroupingEqual

## Location
src/backend/executor/execExpr.c: 3957 - 4113

## Overview
Builds an equality expression that can be evaluated using ExecQual(), returning true if the expression context's inner/outer tuples are NOT DISTINCT (i.e., two nulls match, but a null and a non-null don't match).

## Definition


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
- : TupleDesc describing the structure of left (inner) tuples to compare
- : TupleDesc describing the structure of right (outer) tuples to compare
- : TupleTableSlotOps for left tuple operations
- : TupleTableSlotOps for right tuple operations  
- : Integer specifying the number of attributes to examine in the comparison
- : Array of AttrNumber values indicating which column indices to compare
- : Array of Oid values specifying the equality function OIDs to use for each attribute
- : Array of Oid values specifying the collation OIDs for each attribute
- : PlanState pointer to the parent executor node

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
  - lappend_int
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
  - execTuplesMatchPrepare
  - BuildTupleHashTableExt
  - [ExecInitSubPlan](ExecInitSubPlan.md)

## Notes and Other Information
- Located in src/backend/executor/execExpr.c (lines 3957-4113)
- Implements NOT DISTINCT semantics where NULL = NULL is true
- Optimizes comparison order by starting with the last field (most significant for sorted input)
- Performs security checks by validating function execution permissions
- Returns NULL when numCols is 0, indicating all comparisons should return true
- Uses short-circuit evaluation via QUAL opcodes to exit early on mismatches
- Essential for proper GROUP BY behavior with NULL values in PostgreSQL