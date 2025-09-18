# window_percent_rank_support

## Location
src/backend/utils/adt/windowfuncs.c: 288 - 329

## Overview
A PostgreSQL prosupport function that provides optimization hints and monotonicity information for the window_percent_rank() window function to the query planner and executor.

## Definition
```c
Datum window_percent_rank_support(PG_FUNCTION_ARGS)
```

## Detailed Description
The `window_percent_rank_support` function is a support function that helps PostgreSQL's query planner and executor optimize the execution of `PERCENT_RANK()` window functions. It handles two main types of support requests:

1. **Monotonicity Information (SupportRequestWFuncMonotonic)**: Informs the planner that percent_rank() is a monotonically increasing function, which enables certain optimizations like early termination of window processing and incremental sort optimizations.

2. **Frame Options Optimization (SupportRequestOptimizeWindowClause)**: Optimizes the window frame specification by setting it to use ROWS frame type instead of the default RANGE, and specifying UNBOUNDED PRECEDING to CURRENT ROW. This optimization is safe because percent_rank() doesn't actually use the frame - it only depends on the ORDER BY clause and the total partition size.

The function uses the same optimization strategy as other ranking window functions, allowing the executor to avoid expensive peer row comparisons since the frame boundaries don't affect the percent_rank calculation.

## Parameters / Member Variables
- Takes a support request node as input through PG_FUNCTION_ARGS
- `rawreq`: Node pointer containing the specific support request type

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (macro to extract pointer argument)
  - IsA (macro to check node type)
  - MONOTONICFUNC_INCREASING (constant indicating increasing monotonicity)
  - FRAMEOPTION_* constants (frame option flags)
  - PG_RETURN_POINTER (macro to return pointer result)
- Called from (representative examples):
  - No direct references found (called by PostgreSQL's support function framework)

## Notes and Other Information
- Part of PostgreSQL's function support infrastructure for query optimization
- The frame options optimization saves the executor from checking for peer rows since percent_rank() is unaffected by frame boundaries
- Uses the same frame optimization strategy as row_number's and dense_rank's support functions
- Returns NULL for unsupported request types
- The monotonicity information enables optimizations like 'Incremental Sort' and early termination in certain query patterns
- Support functions are automatically called by PostgreSQL during query planning phases
- The optimization is particularly beneficial for percent_rank() since it needs to access the entire partition anyway to determine the total row count