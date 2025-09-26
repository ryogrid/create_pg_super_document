# Agg

## Location
src/include/nodes/plannodes.h: 996 - 1032

## Overview
Agg is a fundamental plan node that implements aggregation operations in PostgreSQL, supporting both plain aggregates (without GROUP BY) and grouped aggregation with various execution strategies including hash-based and sort-based approaches.

## Definition

```c
typedef struct Agg
{
	Plan		plan;

	/* basic strategy, see nodes.h */
	AggStrategy aggstrategy;

	/* agg-splitting mode, see nodes.h */
	AggSplit	aggsplit;

	/* number of grouping columns */
	int			numCols;

	/* their indexes in the target list */
	AttrNumber *grpColIdx pg_node_attr(array_size(numCols));

	/* equality operators to compare with */
	Oid		   *grpOperators pg_node_attr(array_size(numCols));
	Oid		   *grpCollations pg_node_attr(array_size(numCols));

	/* estimated number of groups in input */
	long		numGroups;

	/* for pass-by-ref transition data */
	uint64		transitionSpace;

	/* IDs of Params used in Aggref inputs */
	Bitmapset  *aggParams;

	/* Note: planner provides numGroups & aggParams only in HASHED/MIXED case */

	/* grouping sets to use */
	List	   *groupingSets;

	/* chained Agg/Sort nodes */
	List	   *chain;
} Agg;
```
## Detailed Description
The Agg node is PostgreSQL's primary mechanism for implementing aggregate functions and GROUP BY operations. It can handle both simple aggregation (like COUNT(*) over all rows) and grouped aggregation (like GROUP BY clauses). The node supports multiple execution strategies: sorted aggregation (which requires presorted input) and hashed aggregation (which uses an internal hash table). The node dynamically determines which aggregate functions to compute by scanning its target list and qualifiers during executor startup. It also supports advanced features like grouping sets, partial aggregation for parallel processing, and aggregate splitting modes.

## Parameters / Member Variables
- : Base Plan structure containing common plan node information
- : Execution strategy (AGG_PLAIN, AGG_SORTED, AGG_HASHED, AGG_MIXED)
- : Aggregate splitting mode for parallel aggregation (AGGSPLIT_SIMPLE, AGGSPLIT_INITIAL_SERIAL, etc.)
- : Number of grouping columns (0 for plain aggregation)
- : Array of attribute numbers for grouping columns
- : Array of OIDs for equality operators used in grouping
- : Array of OIDs for collations used in grouping
- : Estimated number of groups (used for hash table sizing)
- : Estimated memory needed for transition data
- : Bitmap of parameter IDs used in aggregate expressions
- : List of grouping sets for advanced GROUP BY operations
- : List of chained Agg/Sort nodes for complex aggregation plans

## Dependencies
- Functions called/Symbols referenced:
  - Plan (base structure)
  - AggStrategy (aggregation strategy enum)
  - AggSplit (aggregate splitting mode enum)
  - AttrNumber
  - Oid
  - Bitmapset
  - List

- Called from (representative examples):
  - ExecInitAgg (executor/nodeAgg.c:3173)
  - create_agg_plan (optimizer/plan/createplan.c:2311)
  - make_agg (optimizer/plan/createplan.c:6600)
  - show_agg_keys (commands/explain.c:2610)
  - create_groupingsets_plan (optimizer/plan/createplan.c:2395)
  - agg_retrieve_direct (executor/nodeAgg.c:2196)

## Notes and Other Information
- The Agg node can operate without any actual aggregate functions if they are optimized away by constant folding
- Hash-based aggregation is generally more efficient for unsorted input, while sorted aggregation is preferred when input is already ordered
- The node supports partial aggregation for parallel query execution where multiple workers perform initial aggregation before final combining
- Grouping sets functionality allows for complex SQL features like ROLLUP, CUBE, and GROUPING SETS
- Memory usage is carefully managed with work_mem limits, potentially spilling hash tables to disk when necessary
- The transitionSpace field helps estimate memory requirements for variable-length transition data
- Chain field supports multi-level aggregation plans for complex queries