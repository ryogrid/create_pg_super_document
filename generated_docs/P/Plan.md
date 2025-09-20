# Plan

## Location
[src/include/nodes/plannodes.h:119-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L119-L172)

## Overview
Plan is the abstract base structure for all PostgreSQL execution plan nodes, containing common fields used by all plan node types including cost estimates, parallelization info, and structural data.

## Definition

```c
typedef struct Plan
{
	pg_node_attr(abstract, no_equal, no_query_jumble)

	NodeTag		type;

	/*
	 * estimated execution costs for plan (see costsize.c for more info)
	 */
	Cost		startup_cost;	/* cost expended before fetching any tuples */
	Cost		total_cost;		/* total cost (assuming all tuples fetched) */

	/*
	 * planner's estimate of result size of this plan step
	 */
	Cardinality plan_rows;		/* number of rows plan is expected to emit */
	int			plan_width;		/* average row width in bytes */

	/*
	 * information needed for parallel query
	 */
	bool		parallel_aware; /* engage parallel-aware logic? */
	bool		parallel_safe;	/* OK to use as part of parallel plan? */

	/*
	 * information needed for asynchronous execution
	 */
	bool		async_capable;	/* engage asynchronous-capable logic? */

	/*
	 * Common structural data for all Plan types.
	 */
	int			plan_node_id;	/* unique across entire final plan tree */
	List	   *targetlist;		/* target list to be computed at this node */
	List	   *qual;			/* implicitly-ANDed qual conditions */
	struct Plan *lefttree;		/* input plan tree(s) */
	struct Plan *righttree;
	List	   *initPlan;		/* Init Plan nodes (un-correlated expr
								 * subselects) */

	/*
	 * Information for management of parameter-change-driven rescanning
	 *
	 * extParam includes the paramIDs of all external PARAM_EXEC params
	 * affecting this plan node or its children.  setParam params from the
	 * node's initPlans are not included, but their extParams are.
	 *
	 * allParam includes all the extParam paramIDs, plus the IDs of local
	 * params that affect the node (i.e., the setParams of its initplans).
	 * These are _all_ the PARAM_EXEC params that affect this node.
	 */
	Bitmapset  *extParam;
	Bitmapset  *allParam;
} Plan;
```
## Detailed Description
Plan serves as the abstract superclass for all execution plan node types in PostgreSQL. It contains the common data that all plan nodes need, including cost information used by the planner, cardinality estimates, parallelization capabilities, and structural relationships between nodes.

The Plan structure is designed so that all specific plan node types (like SeqScan, HashJoin, etc.) have Plan as their first field, allowing for safe casting between specific plan types and the generic Plan type. This inheritance-like pattern is commonly used throughout PostgreSQL's codebase.

The structure includes cost estimates that guide the planner's decisions, parallel execution metadata, and parameter tracking for efficient plan re-execution. The tree structure is maintained through lefttree and righttree pointers, with additional initPlan nodes for uncorrelated subqueries.

## Parameters / Member Variables
- `type`: Node tag identifying the specific plan node type
- `startup_cost`: Estimated cost before returning the first tuple
- `total_cost`: Estimated total cost if all tuples are fetched
- `plan_rows`: Planner's estimate of the number of rows this node will produce
- `plan_width`: Average width in bytes of rows produced by this node
- `parallel_aware`: True if this node can take advantage of parallel execution
- `parallel_safe`: True if this node is safe to execute in parallel with other nodes
- `async_capable`: True if this node supports asynchronous execution
- `plan_node_id`: Unique identifier for this node within the plan tree
- `*targetlist`: List of expressions to be computed and returned by this node
- `*qual`: List of qualification conditions (WHERE clauses) applied at this node
- `*lefttree`: Left child plan node (primary input for most node types)
- `*righttree`: Right child plan node (used by join nodes, etc.)
- `*initPlan`: List of uncorrelated subquery plans that must execute first
- `*extParam`: Set of external PARAM_EXEC parameter IDs affecting this node
- `*allParam`: Set of all PARAM_EXEC parameter IDs affecting this node (external + local)
## Dependencies
- Functions called/Symbols referenced:
  - Cost
  - Cardinality
  - NodeTag
  - [List](../L/List.md)
  - [Bitmapset](../B/Bitmapset.md)

- Called from (representative examples):
  - This is an abstract base structure used by all specific plan node types
  - Referenced through PlannedStmt.planTree
  - Used throughout the executor via generic Plan* pointers
  - Cast to specific plan types (SeqScan*, HashJoin*, etc.) in executor nodes

## Notes and Other Information
- This is an abstract structure - no Plan nodes are directly instantiated
- All concrete plan node types must have Plan as their first field for safe casting
- The cost estimates are used by the planner to choose between alternative plans
- Parameter tracking (extParam/allParam) enables efficient plan re-execution when only parameter values change
- Parallel execution capabilities are determined at planning time and stored in parallel_aware/parallel_safe flags
- The plan tree structure allows for complex nested operations through lefttree/righttree relationships
- initPlan nodes execute once before the main plan tree execution begins