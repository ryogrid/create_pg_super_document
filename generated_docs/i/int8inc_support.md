# int8inc_support

## Location
src/backend/utils/adt/int8.c: 826 - 865

## Overview
A prosupport function for int8inc() and int8inc_any() that provides window function monotonicity analysis for query optimization.

## Definition
Datum int8inc_support(PG_FUNCTION_ARGS)

## Detailed Description
int8inc_support is a support function that analyzes the monotonic properties of int8inc operations within window functions. It examines window frame options and ordering clauses to determine whether the increment function behaves monotonically (always increasing, decreasing, or both) within a given window context. This information is used by the PostgreSQL query planner for optimization purposes, particularly for window function processing.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Contains a Node pointer to a SupportRequestWFuncMonotonic request structure

## Dependencies
- Functions called/Symbols referenced:
  - SupportRequestWFuncMonotonic
  - MonotonicFunction
  - MONOTONICFUNC_NONE
  - MONOTONICFUNC_BOTH
  - MONOTONICFUNC_INCREASING
  - MONOTONICFUNC_DECREASING
  - FRAMEOPTION_START_UNBOUNDED_PRECEDING
  - FRAMEOPTION_END_UNBOUNDED_FOLLOWING
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
This function specifically handles SupportRequestWFuncMonotonic requests to determine monotonicity characteristics based on window frame options. When no ORDER BY clause is present, all rows are considered peers and the function is both monotonically increasing and decreasing. With frame bounds at window start or end, the function determines appropriate monotonic behavior for optimization. The function is defined in src/backend/utils/adt/int8.c:826-865.