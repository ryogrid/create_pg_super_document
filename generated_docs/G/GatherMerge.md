# GatherMerge

## Location
[src/include/nodes/plannodes.h:1155-1187](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L1155-L1187)

## Overview
The GatherMerge node implements parallel query execution with ordered results by launching multiple worker processes to execute a sorted subplan and merging their sorted outputs into a single ordered stream.

## Definition

```c
typedef struct GatherMerge
{
	Plan		plan;

	/* planned number of worker processes */
	int			num_workers;

	/* ID of Param that signals a rescan, or -1 */
	int			rescan_param;

	/* remaining fields are just like the sort-key info in struct Sort */

	/* number of sort-key columns */
	int			numCols;

	/* their indexes in the target list */
	AttrNumber *sortColIdx pg_node_attr(array_size(numCols));

	/* OIDs of operators to sort them by */
	Oid		   *sortOperators pg_node_attr(array_size(numCols));

	/* OIDs of collations */
	Oid		   *collations pg_node_attr(array_size(numCols));

	/* NULLS FIRST/LAST directions */
	bool	   *nullsFirst pg_node_attr(array_size(numCols));

	/*
	 * param id's of initplans which are referred at gather merge or one of
	 * it's child node
	 */
	Bitmapset  *initParam;
} GatherMerge;
```
## Detailed Description
The GatherMerge node extends the Gather node concept by maintaining sort order across parallel execution. It launches multiple worker processes that each execute a subplan designed to produce sorted output, then uses a binary heap-based merge algorithm to combine the sorted streams from all workers (plus optionally the leader) into a single ordered result stream.

Key operational characteristics:
- Each worker produces a sorted stream according to the specified sort keys
- A binary heap efficiently manages the merge of multiple sorted streams
- The merge preserves the overall sort order while leveraging parallelism
- Sort keys include operators, collations, and null ordering specifications
- The leader process can participate in both execution and result merging

The node is essential for parallel execution of queries requiring ordered results, such as ORDER BY clauses, merge joins, and other sort-dependent operations.

## Parameters / Member Variables
- : Base Plan structure containing common plan node information
- : The planned number of parallel worker processes to launch
- : Parameter ID used to signal rescans to parallel-aware child nodes (-1 if not needed)
- : Number of columns in the sort key specification
- : Array of target list column indexes to use as sort keys
- : Array of comparison operator OIDs for each sort column
- : Array of collation OIDs for locale-specific sorting of each column
- : Array of boolean flags indicating NULL ordering (NULLS FIRST vs NULLS LAST) for each column
- : Bitmap set of parameter IDs for InitPlans referenced by this node or its children

## Dependencies
- Functions called/Symbols referenced:
  - Plan (base structure)
  - AttrNumber
  - Oid
  - Bitmapset
  - SortSupport
  - binaryheap
- Called from (representative examples):
  - ExecInitGatherMerge
  - ExecGatherMerge
  - ExecReScanGatherMerge
  - create_gather_merge_plan
  - gather_merge_setup

## Notes and Other Information
- The GatherMerge node combines the benefits of parallel execution with ordered result requirements
- Uses a binary heap data structure for efficiently merging multiple sorted streams
- Sort key information is identical to that used by the Sort node, enabling consistent sorting behavior
- Each worker must produce output sorted according to the same criteria
- The merge algorithm ensures that the overall result maintains the specified sort order
- More complex than regular Gather due to the additional sorting and merging logic
- Critical for enabling parallelism in queries with ORDER BY clauses and sort-dependent operations
- Worker processes communicate sorted results through tuple queues, which are then merged by the leader