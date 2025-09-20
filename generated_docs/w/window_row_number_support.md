# window_row_number_support

## Location
[src/backend/utils/adt/windowfuncs.c:98-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L98-L137)

## Overview
A prosupport function that provides optimization hints and metadata for the ROW_NUMBER() window function to the PostgreSQL query planner.

## Definition
```c
Datum window_row_number_support(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a prosupport (procedural support) function for the ROW_NUMBER() window function. PostgreSQL's query planner calls this function to gather optimization information about how ROW_NUMBER() behaves. The function handles two types of support requests:

1. **Monotonicity Request**: Informs the planner that ROW_NUMBER() is monotonically increasing, which helps with certain optimizations and query transformations.

2. **Window Clause Optimization Request**: Suggests optimal frame options for ROW_NUMBER(). Since ROW_NUMBER() simply increments by 1 for each row regardless of ORDER BY values, it can always use "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" framing instead of the default RANGE framing. This optimization avoids the overhead of checking for peer rows since ROW_NUMBER() doesn't care about peer relationships.

The function uses PostgreSQL's support request infrastructure to communicate these optimizations to the query planner.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER
  - IsA (macro)
  - PG_RETURN_POINTER
  - SupportRequestWFuncMonotonic (struct)
  - SupportRequestOptimizeWindowClause (struct)
  - MONOTONICFUNC_INCREASING
  - FRAMEOPTION_NONDEFAULT
  - FRAMEOPTION_ROWS
  - FRAMEOPTION_START_UNBOUNDED_PRECEDING
  - FRAMEOPTION_END_CURRENT_ROW
- Called from (representative examples):
  - PostgreSQL query planner during optimization phase

## Notes and Other Information
- This is a prosupport function registered in PostgreSQL's system catalogs alongside ROW_NUMBER()
- The function enables important query optimizations by providing metadata about ROW_NUMBER()'s behavior
- The frame optimization from RANGE to ROWS can significantly improve performance by eliminating peer row comparisons
- Returns NULL for unrecognized support request types
- The monotonicity information helps with window function reordering and other planner optimizations