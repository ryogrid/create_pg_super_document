# BTShared

## Location
[src/backend/access/nbtree/nbtsort.c:94-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L94-L151)

## Overview
BTShared is a structure that contains status information for B-tree index builds performed in parallel, allocated in a dynamic shared memory segment to coordinate between the leader and worker processes.

## Definition

```c
typedef struct BTShared
{
	/*
	 * These fields are not modified during the sort.  They primarily exist
	 * for the benefit of worker processes that need to create BTSpool state
	 * corresponding to that used by the leader.
	 */
	Oid			heaprelid;
	Oid			indexrelid;
	bool		isunique;
	bool		nulls_not_distinct;
	bool		isconcurrent;
	int			scantuplesortstates;

	/*
	 * workersdonecv is used to monitor the progress of workers.  All parallel
	 * participants must indicate that they are done before leader can use
	 * mutable state that workers maintain during scan (and before leader can
	 * proceed to tuplesort_performsort()).
	 */
	ConditionVariable workersdonecv;

	/*
	 * mutex protects all fields before heapdesc.
	 *
	 * These fields contain status information of interest to B-Tree index
	 * builds that must work just the same when an index is built in parallel.
	 */
	slock_t		mutex;

	/*
	 * Mutable state that is maintained by workers, and reported back to
	 * leader at end of parallel scan.
	 *
	 * nparticipantsdone is number of worker processes finished.
	 *
	 * reltuples is the total number of input heap tuples.
	 *
	 * havedead indicates if RECENTLY_DEAD tuples were encountered during
	 * build.
	 *
	 * indtuples is the total number of tuples that made it into the index.
	 *
	 * brokenhotchain indicates if any worker detected a broken HOT chain
	 * during build.
	 */
	int			nparticipantsdone;
	double		reltuples;
	bool		havedead;
	double		indtuples;
	bool		brokenhotchain;

	/*
	 * ParallelTableScanDescData data follows. Can't directly embed here, as
	 * implementations of the parallel table scan desc interface might need
	 * stronger alignment.
	 */
} BTShared;
```
## Detailed Description
BTShared serves as the central coordination structure for parallel B-tree index construction. It is allocated in dynamic shared memory and shared between the leader process and all worker processes participating in the parallel index build. The structure is divided into immutable fields that are set once during initialization and mutable fields that are updated by workers during the scan phase and aggregated by the leader.

The structure includes a condition variable for synchronizing worker completion and a spinlock mutex to protect access to mutable state. Workers report their progress through the mutable fields, which the leader aggregates to maintain overall build statistics.

## Parameters / Member Variables
- `heaprelid`: OID of the heap relation being indexed
- `indexrelid`: OID of the index relation being built
- `isunique`: Whether the index enforces uniqueness constraints
- `nulls_not_distinct`: Whether NULL values are considered distinct in unique indexes
- `isconcurrent`: Whether this is a concurrent index build
- `scantuplesortstates`: Number of tuplesort states for scanning
- `workersdonecv`: Condition variable used to monitor worker progress completion
- `mutex`: Spinlock protecting all mutable fields below
- `nparticipantsdone`: Number of worker processes that have finished
- `reltuples`: Total number of input heap tuples processed
- `havedead`: Whether RECENTLY_DEAD tuples were encountered during build
- `indtuples`: Total number of tuples that made it into the index
- `brokenhotchain`: Whether any worker detected a broken HOT chain during build
## Dependencies
- Functions called/Symbols referenced:
  - ConditionVariable
  - [slock_t](../s/slock_t.md)
- Called from (representative examples):
  - [BTLeader](BTLeader.md)
  - [_bt_begin_parallel](../b/_bt_begin_parallel.md)
  - [_bt_parallel_estimate_shared](../b/_bt_parallel_estimate_shared.md)
  - [_bt_parallel_heapscan](../b/_bt_parallel_heapscan.md)
  - [_bt_parallel_build_main](../b/_bt_parallel_build_main.md)
  - [_bt_parallel_scan_and_sort](../b/_bt_parallel_scan_and_sort.md)

## Notes and Other Information
BTShared is designed specifically for parallel index builds and contains a separate tuplesort TOC entry that is private to tuplesort.c but allocated by the nbtsort module. The structure layout ensures that ParallelTableScanDescData follows immediately after, with consideration for alignment requirements of the parallel table scan descriptor interface. The mutable state fields are only safe to access after all workers have indicated completion through the workersdonecv condition variable.