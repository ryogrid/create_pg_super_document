# window_ntile_support

## Location
[src/backend/utils/adt/windowfuncs.c:483-527](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L483-L527)

## Overview
This function serves as the prosupport function for the window_ntile() function, providing optimization hints and monotonicity information to PostgreSQL's query planner.

## Definition
```c
Datum window_ntile_support(PG_FUNCTION_ARGS)
```

## Detailed Description
The window_ntile_support function is a support function that handles two types of optimization requests for the ntile() window function:

1. **Monotonicity Information**: It informs the planner that ntile() is monotonically increasing. This is true because the number of buckets cannot change after the first call within a partition, and bucket numbers are assigned sequentially.

2. **Frame Options Optimization**: It optimizes the window frame specification by setting it to use ROWS instead of RANGE (the default), and specifies a frame from unbounded preceding to current row. This optimization saves the executor from having to check for peer rows during execution, similar to the optimization done for row_number().

The function uses PostgreSQL's support function infrastructure to provide these optimizations at query planning time.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing a Node pointer to the support request

## Dependencies
- Functions called/Symbols referenced:
  - [SupportRequestWFuncMonotonic](../S/SupportRequestWFuncMonotonic.md)
  - [SupportRequestOptimizeWindowClause](../S/SupportRequestOptimizeWindowClause.md)  
  - MONOTONICFUNC_INCREASING
  - FRAMEOPTION_NONDEFAULT
  - FRAMEOPTION_ROWS
  - FRAMEOPTION_START_UNBOUNDED_PRECEDING
  - FRAMEOPTION_END_CURRENT_ROW
- Called from (representative examples):
  - Used internally by PostgreSQL's function call infrastructure for ntile() window function

## Notes and Other Information
- This is a prosupport function, meaning it provides metadata and optimization hints rather than performing the actual computation
- The monotonicity information (MONOTONICFUNC_INCREASING) helps the planner with optimizations related to ordering and partitioning
- The frame optimization using ROWS instead of RANGE can significantly improve performance by avoiding peer row comparisons
- The optimization is particularly beneficial for ntile() since it doesn't depend on the actual frame boundaries but rather on the entire partition
- Located in src/backend/utils/adt/windowfuncs.c:483-527