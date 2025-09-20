# BTBuildState

## Location
[src/backend/access/nbtree/nbtsort.c:202-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L202-L223)

## Overview
BTBuildState is the working state structure for B-tree index construction (btbuild) and its callback functions, with each participant process having its own instance during parallel index builds.

## Definition

```c
typedef struct BTBuildState
{
	bool		isunique;
	bool		nulls_not_distinct;
	bool		havedead;
	Relation	heap;
	BTSpool    *spool;

	/*
	 * spool2 is needed only when the index is a unique index. Dead tuples are
	 * put into spool2 instead of spool in order to avoid uniqueness check.
	 */
	BTSpool    *spool2;
	double		indtuples;

	/*
	 * btleader is only present when a parallel index build is performed, and
	 * only in the leader process. (Actually, only the leader has a
	 * BTBuildState.  Workers have their own spool and spool2, though.)
	 */
	BTLeader   *btleader;
} BTBuildState;
```
## Detailed Description
BTBuildState serves as the primary working state container for B-tree index construction operations. Each participant in an index build (whether serial or parallel) maintains its own BTBuildState instance. The structure manages the sorting and spooling operations required during index construction, including special handling for unique indexes through a secondary spool.

In parallel builds, only the leader process has the btleader field populated, while worker processes maintain their own spool and spool2 instances. The structure tracks various build characteristics like uniqueness constraints and dead tuple handling requirements.

## Parameters / Member Variables
- : Whether the index being built enforces uniqueness constraints
- : Whether NULL values are considered distinct in unique indexes
- : Whether RECENTLY_DEAD tuples have been encountered during the build
- : Relation pointer to the heap table being indexed
- : Primary BTSpool instance for tuple sorting and processing
- : Secondary BTSpool instance used only for unique indexes to handle dead tuples separately and avoid uniqueness check conflicts
- : Total count of tuples that have been successfully added to the index
- : Pointer to BTLeader structure, only present in the leader process during parallel index builds (NULL in workers and serial builds)

## Dependencies
- Functions called/Symbols referenced:
  - [BTSpool](BTSpool.md)
  - [BTLeader](BTLeader.md)
- Called from (representative examples):
  - [btbuild](../b/btbuild.md)
  - [_bt_spools_heapscan](../b/_bt_spools_heapscan.md)
  - [_bt_build_callback](../b/_bt_build_callback.md)
  - [_bt_begin_parallel](../b/_bt_begin_parallel.md)
  - [_bt_parallel_heapscan](../b/_bt_parallel_heapscan.md)
  - [_bt_leader_participate_as_worker](../b/_bt_leader_participate_as_worker.md)
  - [_bt_parallel_scan_and_sort](../b/_bt_parallel_scan_and_sort.md)

## Notes and Other Information
BTBuildState is designed to support both serial and parallel index construction workflows. The dual spool architecture (spool and spool2) is specifically designed for unique indexes where dead tuples must be processed separately to avoid false uniqueness violations. In parallel builds, worker processes maintain their own BTBuildState instances but without the btleader field, which is exclusive to the leader process for coordination purposes. The structure serves as the central state container passed to various callback functions during the index building process.