# WindowFuncRunCondition

## Location
[src/include/nodes/primnodes.h:596-616](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L596-L616)

## Overview
WindowFuncRunCondition represents intermediate comparison expressions used by WindowAgg nodes to enable short-circuit execution optimizations during window function evaluation.

## Definition

```c
typedef struct WindowFuncRunCondition
{
	Expr		xpr;

	/* PG_OPERATOR OID of the operator */
	Oid			opno;
	/* OID of collation that operator should use */
	Oid			inputcollid pg_node_attr(query_jumble_ignore);

	/*
	 * true of WindowFunc belongs on the left of the resulting OpExpr or false
	 * if the WindowFunc is on the right.
	 */
	bool		wfunc_left;

	/*
	 * The Expr being compared to the WindowFunc to use in the OpExpr in the
	 * WindowAgg's runCondition
	 */
	Expr	   *arg;
} WindowFuncRunCondition;
```
## Detailed Description
WindowFuncRunCondition is an optimization structure used by the PostgreSQL query executor to enable early termination of window function computation. When the optimizer can determine that a window function's result will not change based on certain conditions, it can create these run conditions to short-circuit the execution process.

This structure encapsulates a comparison operation between a window function result and another expression, allowing the WindowAgg executor node to skip unnecessary computations when the condition indicates that further processing would not change the final result.

## Parameters / Member Variables
- : Base expression node structure (inherited from Expr)
- : OID of the comparison operator from pg_operator catalog (e.g., equality, less than, etc.)
- : Collation OID that the operator should use for comparison operations (ignored in query jumbling)
- : Boolean flag indicating the position of the WindowFunc in the resulting comparison expression - true if WindowFunc is on the left side, false if on the right side
- : The expression being compared against the window function result in the OpExpr used for the run condition

## Dependencies
- Functions called/Symbols referenced:
  - Expr (base expression structure)
  - Oid (for operator and collation references)
  
- Called from (representative examples):
  - find_window_run_conditions (optimizer path planning for creating run conditions)
  - create_one_window_path (query planner when building window execution paths)
  - LIST_WALK and MUTATE (node traversal and transformation functions)

## Notes and Other Information
- This optimization is particularly effective for window functions with RANGE or ROWS frames where early termination can significantly reduce computational overhead
- The structure supports both directions of comparison (WindowFunc op Expr and Expr op WindowFunc) through the wfunc_left flag
- The inputcollid field is marked as query_jumble_ignore for consistent plan caching
- Used internally by the optimizer to create more efficient execution plans for window functions with predictable termination conditions
- Part of PostgreSQL's advanced query optimization for window function performance