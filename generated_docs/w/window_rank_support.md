# window_rank_support

## Location
[src/backend/utils/adt/windowfuncs.c:158-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L158-L199)

## Overview
A prosupport function that provides optimization hints and metadata for the RANK() window function to the PostgreSQL query planner.

## Definition
```c
Datum window_rank_support(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a prosupport (procedural support) function for the RANK() window function. PostgreSQL's query planner calls this function to gather optimization information about how RANK() behaves. The function handles two types of support requests:

1. **Monotonicity Request**: Informs the planner that RANK() is monotonically increasing, which helps with certain optimizations and query transformations.

2. **Window Clause Optimization Request**: Suggests optimal frame options for RANK(). Although RANK() conceptually works with RANGE framing to handle peer rows, its internal implementation actually computes the rank based on row position. The function optimizes the frame to "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" instead of the default RANGE framing. This optimization avoids the overhead of peer row checks during execution while maintaining correct semantics, since RANK() determines peer relationships through its own logic rather than relying on the window frame.

The optimization is particularly effective because RANK() internally calculates its value as if it were computing a row number for non-peer rows, making ROWS framing more efficient than RANGE framing.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro for PostgreSQL function parameter handling
- Receives a `Node *` pointer representing the support request type

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
- This is a prosupport function registered in PostgreSQL's system catalogs alongside RANK()
- The function enables important query optimizations by providing metadata about RANK()'s behavior
- The frame optimization from RANGE to ROWS can significantly improve performance by eliminating peer row comparisons during frame construction
- Returns NULL for unrecognized support request types
- The monotonicity information helps with window function reordering and other planner optimizations
- This optimization is consistent with window_row_number_support() to enable better query planning when both functions are used together