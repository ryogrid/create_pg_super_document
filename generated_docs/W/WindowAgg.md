# WindowAgg

## Location
[src/include/nodes/plannodes.h:1038-1106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L1038-L1106)

## Overview
WindowAgg is a specialized plan node that implements window functions in PostgreSQL, processing data within sliding windows defined by PARTITION BY and ORDER BY clauses with optional frame specifications.

## Definition

```c
typedef struct WindowAgg
{
	Plan		plan;

	/* ID referenced by window functions */
	Index		winref;

	/* number of columns in partition clause */
	int			partNumCols;

	/* their indexes in the target list */
	AttrNumber *partColIdx pg_node_attr(array_size(partNumCols));

	/* equality operators for partition columns */
	Oid		   *partOperators pg_node_attr(array_size(partNumCols));

	/* collations for partition columns */
	Oid		   *partCollations pg_node_attr(array_size(partNumCols));

	/* number of columns in ordering clause */
	int			ordNumCols;

	/* their indexes in the target list */
	AttrNumber *ordColIdx pg_node_attr(array_size(ordNumCols));

	/* equality operators for ordering columns */
	Oid		   *ordOperators pg_node_attr(array_size(ordNumCols));

	/* collations for ordering columns */
	Oid		   *ordCollations pg_node_attr(array_size(ordNumCols));

	/* frame_clause options, see WindowDef */
	int			frameOptions;

	/* expression for starting bound, if any */
	Node	   *startOffset;

	/* expression for ending bound, if any */
	Node	   *endOffset;

	/* qual to help short-circuit execution */
	List	   *runCondition;

	/* runCondition for display in EXPLAIN */
	List	   *runConditionOrig;

	/* these fields are used with RANGE offset PRECEDING/FOLLOWING: */

	/* in_range function for startOffset */
	Oid			startInRangeFunc;

	/* in_range function for endOffset */
	Oid			endInRangeFunc;

	/* collation for in_range tests */
	Oid			inRangeColl;

	/* use ASC sort order for in_range tests? */
	bool		inRangeAsc;

	/* nulls sort first for in_range tests? */
	bool		inRangeNullsFirst;

	/*
	 * false for all apart from the WindowAgg that's closest to the root of
	 * the plan
	 */
	bool		topWindow;
} WindowAgg;
```
## Detailed Description
The WindowAgg node implements SQL window functions such as ROW_NUMBER(), RANK(), SUM() OVER(), and LAG()/LEAD(). It processes input data partitioned by specified columns and ordered within each partition. The node maintains a sliding window frame that can be defined using ROWS or RANGE clauses with PRECEDING/FOLLOWING boundaries. It supports both bounded and unbounded frames, and can efficiently handle peer groups (rows with identical values in ORDER BY columns). The node can run multiple window functions simultaneously if they share the same window specification, optimizing execution by avoiding redundant sorting and partitioning operations.

## Parameters / Member Variables
- : Base Plan structure containing common plan node information
- : Window reference ID used to identify the window specification
- : Number of columns in the PARTITION BY clause
- : Array of attribute numbers for partition columns
- : Array of equality operators for partition column comparisons
- : Array of collations for partition columns
- : Number of columns in the ORDER BY clause within the window
- : Array of attribute numbers for ordering columns
- : Array of comparison operators for ordering columns
- : Array of collations for ordering columns
- : Bit flags defining frame type (ROWS/RANGE/GROUPS) and bounds
- : Expression defining the starting boundary of the frame
- : Expression defining the ending boundary of the frame
- : Optimized conditions for early termination
- : Original run condition for EXPLAIN output
- : Function for RANGE frame start boundary calculations
- : Function for RANGE frame end boundary calculations
- : Collation for in-range function calls
- : Sort order for range comparisons
- : NULL handling for range comparisons
- : True if this is the topmost WindowAgg node in the plan

## Dependencies
- Functions called/Symbols referenced:
  - [Plan](../P/Plan.md) (base structure)
  - Index
  - AttrNumber
  - Oid
  - [Node](../N/Node.md)
  - [List](../L/List.md)

- Called from (representative examples):
  - [ExecInitWindowAgg](../E/ExecInitWindowAgg.md) (executor/nodeWindowAgg.c:2374)
  - [create_windowagg_plan](../c/create_windowagg_plan.md) (optimizer/plan/createplan.c:2619)
  - [make_windowagg](../m/make_windowagg.md) (optimizer/plan/createplan.c:6636)
  - [begin_partition](../b/begin_partition.md) (executor/nodeWindowAgg.c:1083)
  - [update_frameheadpos](../u/update_frameheadpos.md) (executor/nodeWindowAgg.c:1487)
  - [WinRowsArePeers](WinRowsArePeers.md) (executor/nodeWindowAgg.c:3256)

## Notes and Other Information
- [WindowAgg](WindowAgg.md) nodes require their input to be sorted by partition columns first, then by ordering columns within each partition
- The node can handle complex frame specifications including UNBOUNDED PRECEDING/FOLLOWING, CURRENT ROW, and numeric/interval offsets
- Multiple window functions with identical window specifications can be computed by a single WindowAgg node for efficiency
- The topWindow field helps optimize execution when multiple WindowAgg nodes are stacked
- RANGE frames with PRECEDING/FOLLOWING require special in_range functions for proper boundary calculations
- The node supports runtime optimization through runCondition to skip unnecessary computation
- Frame boundaries can be dynamically computed using expressions, not just constant offsets
- Peer detection is crucial for functions like RANK() and DENSE_RANK() that treat equal values specially