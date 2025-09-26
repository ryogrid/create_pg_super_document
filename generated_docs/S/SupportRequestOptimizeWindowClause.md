# SupportRequestOptimizeWindowClause

## Location
[src/include/nodes/supportnodes.h:333-344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/supportnodes.h#L333-L344)

## Overview
A support structure that allows window functions to optimize their associated WindowClause frame options by suggesting more efficient alternatives that produce identical results, primarily enabling RANGE-to-ROWS optimizations for performance improvement.

## Definition

```c
typedef struct SupportRequestOptimizeWindowClause
{
	NodeTag		type;

	/* Input fields: */
	WindowFunc *window_func;	/* Pointer to the window function data */
	struct WindowClause *window_clause; /* Pointer to the window clause data */

	/* Input/Output fields: */
	int			frameOptions;	/* New frameOptions, or left untouched if no
								 * optimizations are possible. */
} SupportRequestOptimizeWindowClause;
```
## Detailed Description
This structure enables window function support functions to optimize WindowClause frame options without affecting the function's result semantics. The primary optimization target is converting RANGE-based frames to ROWS-based frames, which reduces computational overhead in nodeWindowAgg.c by eliminating the need to check for peer rows.

Since RANGE is included in default frame options, functions like row_number() that are unaffected by frame boundaries can benefit from this optimization. The window function's support function analyzes whether the current frame options can be modified to more efficient alternatives while maintaining identical results.

The planner coordinates this optimization across all WindowFuncs using the same WindowClause, ensuring all functions agree on the optimized frame options before making any changes. If any WindowFunc lacks a support function or disagrees with the optimization, no changes are made.

## Parameters / Member Variables
- : NodeTag identifier for this structure type
- : Input field pointing to the WindowFunc being analyzed for frame optimization opportunities
- : Input field pointing to the WindowClause containing current frame options and other window specifications
- : Input/Output field initialized with WindowClause.frameOptions; support functions modify this to suggest optimized frame options, or leave unchanged if no optimizations are possible

## Dependencies
- Functions called/Symbols referenced:
  - [WindowFunc](../W/WindowFunc.md)
  - [WindowClause](../W/WindowClause.md)
- Called from (representative examples):
  - [optimize_window_clauses](../o/optimize_window_clauses.md)
  - [window_row_number_support](../w/window_row_number_support.md)
  - [window_rank_support](../w/window_rank_support.md)
  - [window_dense_rank_support](../w/window_dense_rank_support.md)
  - [window_percent_rank_support](../w/window_percent_rank_support.md)
  - [window_cume_dist_support](../w/window_cume_dist_support.md)
  - [window_ntile_support](../w/window_ntile_support.md)

## Notes and Other Information
- This optimization primarily targets the RANGE vs ROWS distinction, where ROWS is generally more efficient as it avoids peer row checking
- The structure is designed for future extensibility beyond frameOptions, allowing additional WindowClause optimizations
- Support functions must ensure optimizations preserve result correctness - they are responsible for semantic equivalence
- The planner only applies optimizations when all WindowFuncs using the same WindowClause agree on the changes
- Common optimization example: row_number() can use ROWS instead of RANGE since frame boundaries don't affect row numbering
- Frame options are defined as bit flags (FRAMEOPTION_RANGE, FRAMEOPTION_ROWS, etc.) allowing efficient manipulation and comparison
- This optimization is part of PostgreSQL's broader window function performance enhancement framework