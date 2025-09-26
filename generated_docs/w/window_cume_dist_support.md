# window_cume_dist_support

## Location
[src/backend/utils/adt/windowfuncs.c:371-410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L371-L410)

## Overview
This function serves as the prosupport function for the window_cume_dist() function, providing optimization hints and monotonicity information to PostgreSQL's query planner.

## Definition

```c
Datum
window_cume_dist_support(PG_FUNCTION_ARGS)
```
## Detailed Description
The window_cume_dist_support function is a support function that handles two types of optimization requests for the cume_dist() window function:

1. **Monotonicity Information**: It informs the planner that cume_dist() is monotonically increasing, which allows for certain query optimizations.

2. **Frame Options Optimization**: It optimizes the window frame specification by setting it to use ROWS instead of RANGE (the default), and specifies a frame from unbounded preceding to current row. This optimization saves the executor from having to check for peer rows during execution.

The function uses PostgreSQL's support function infrastructure to provide these optimizations at query planning time.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing a Node pointer to the support request

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
  - Used internally by PostgreSQL's function call infrastructure for cume_dist() window function

## Notes and Other Information
- This is a prosupport function, meaning it provides metadata and optimization hints rather than performing the actual computation
- The monotonicity information (MONOTONICFUNC_INCREASING) helps the planner with optimizations related to ordering and partitioning
- The frame optimization using ROWS instead of RANGE can significantly improve performance by avoiding peer row comparisons
- Located in src/backend/utils/adt/windowfuncs.c:371-410