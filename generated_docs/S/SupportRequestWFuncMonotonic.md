# SupportRequestWFuncMonotonic

## Location
src/include/nodes/supportnodes.h: 290 - 300

## Overview
A support structure used to request monotonic property analysis of window functions from their prosupport functions, enabling query optimization through understanding whether window functions produce monotonically increasing, decreasing, or constant values.

## Definition

```c
typedef struct SupportRequestWFuncMonotonic
{
	NodeTag		type;

	/* Input fields: */
	WindowFunc *window_func;	/* Pointer to the window function data */
	struct WindowClause *window_clause; /* Pointer to the window clause data */

	/* Output fields: */
	MonotonicFunction monotonic;
} SupportRequestWFuncMonotonic;
```
## Detailed Description
This structure is part of PostgreSQL's support function infrastructure for window function optimization. When the planner encounters an OpExpr qualification that directly references a window function in a subquery, it can populate this structure and pass it to the window function's prosupport function to determine the function's monotonic properties within a partition.

The monotonic analysis helps optimize query execution by understanding the behavior patterns of window functions:
- Monotonically increasing functions never return lower values than previous calls (e.g., row_number())
- Monotonically decreasing functions never return higher values than previous calls  
- Functions that are both increasing and decreasing return constant values (e.g., COUNT(*) OVER() without ORDER BY)
- Non-monotonic functions have no predictable ordering constraints

The analysis is performed per partition, and "previous call" refers to earlier calls to the same WindowFunc within the same window partition.

## Parameters / Member Variables
- : NodeTag identifier for this structure type
- : Input field pointing to the WindowFunc structure being analyzed for monotonic properties
- : Input field pointing to the WindowClause containing frame bounds and partitioning information
- : Output field that receives the MonotonicFunction result indicating the function's monotonic behavior (MONOTONICFUNC_NONE, MONOTONICFUNC_INCREASING, MONOTONICFUNC_DECREASING, or MONOTONICFUNC_BOTH)

## Dependencies
- Functions called/Symbols referenced:
  - WindowFunc
  - WindowClause  
  - MonotonicFunction
- Called from (representative examples):
  - find_window_run_conditions
  - int8inc_support
  - window_row_number_support
  - window_rank_support
  - window_dense_rank_support
  - window_percent_rank_support
  - window_cume_dist_support
  - window_ntile_support

## Notes and Other Information
- This structure is specifically designed for window function optimization and is not used for regular aggregate functions
- The monotonic analysis is critical for enabling "window function run conditions" optimization that can skip redundant computations
- Examples of monotonic functions include row_number() (increasing), running totals with unbounded preceding frames, and constant aggregates over entire partitions
- The structure is defined in src/include/nodes/supportnodes.h and is part of the broader support function framework introduced for advanced function optimizations