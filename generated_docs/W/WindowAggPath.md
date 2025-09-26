# WindowAggPath

## Location
[src/include/nodes/pathnodes.h:2318-2327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2318-L2327)

## Overview
WindowAggPath represents a path for computing window functions in PostgreSQL, handling operations like ROW_NUMBER(), RANK(), and aggregate functions with OVER clauses.

## Definition

```c
typedef struct WindowAggPath
{
	Path		path;
	Path	   *subpath;		/* path representing input source */
	WindowClause *winclause;	/* WindowClause we'll be using */
	List	   *qual;			/* lower-level WindowAgg runconditions */
	List	   *runCondition;	/* OpExpr List to short-circuit execution */
	bool		topwindow;		/* false for all apart from the WindowAgg
								 * that's closest to the root of the plan */
} WindowAggPath;
```
## Detailed Description
WindowAggPath represents the execution path for window function computation in PostgreSQL's query planner. Window functions perform calculations across a set of table rows that are related to the current row, such as running totals, rankings, or moving averages. This path type encapsulates the strategy for efficiently executing window functions, including the input data source, the specific window clause defining the window specification, and optimization features like run conditions that can short-circuit execution when possible.

## Parameters / Member Variables
- `path`: Base Path structure containing cost estimates, output row count, and other common path properties
- `*subpath`: Pointer to the input path that provides the source data for window function computation
- `*winclause`: Pointer to the WindowClause structure that defines the window specification (PARTITION BY, ORDER BY, frame clause)
- `*qual`: List of lower-level WindowAgg run conditions used for optimization and filtering
- `*runCondition`: List of OpExpr structures that enable short-circuit execution when conditions are not met
- `topwindow`: Boolean flag indicating whether this is the top-level WindowAgg node closest to the plan root (used for optimization)
## Dependencies
- Functions called/Symbols referenced:
  - [WindowClause](WindowClause.md)
- Called from (representative examples):
  - [create_windowagg_plan](../c/create_windowagg_plan.md)
  - [create_windowagg_path](../c/create_windowagg_path.md)
  - [create_plan_recurse](../c/create_plan_recurse.md)

## Notes and Other Information
- The topwindow flag helps optimize nested window function execution by identifying the outermost window operation
- Run conditions enable early termination of window function computation when certain conditions are met
- Window functions require careful ordering of input data, which is handled through the subpath
- Multiple window functions with the same window specification can be computed efficiently in a single WindowAggPath