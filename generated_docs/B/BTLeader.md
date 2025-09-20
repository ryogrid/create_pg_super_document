# BTLeader

## Location
[src/backend/access/nbtree/nbtsort.c:165-194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L165-L194)

## Overview
BTLeader is a structure that contains status and convenience pointers for the leader process in parallel B-tree index builds, providing access to shared state and coordinating worker processes.

## Definition

```c
typedef struct BTLeader
{
	/* parallel context itself */
	ParallelContext *pcxt;

	/*
	 * nparticipanttuplesorts is the exact number of worker processes
	 * successfully launched, plus one leader process if it participates as a
	 * worker (only DISABLE_LEADER_PARTICIPATION builds avoid leader
	 * participating as a worker).
	 */
	int			nparticipanttuplesorts;

	/*
	 * Leader process convenience pointers to shared state (leader avoids TOC
	 * lookups).
	 *
	 * btshared is the shared state for entire build.  sharedsort is the
	 * shared, tuplesort-managed state passed to each process tuplesort.
	 * sharedsort2 is the corresponding btspool2 shared state, used only when
	 * building unique indexes.  snapshot is the snapshot used by the scan iff
	 * an MVCC snapshot is required.
	 */
	BTShared   *btshared;
	Sharedsort *sharedsort;
	Sharedsort *sharedsort2;
	Snapshot	snapshot;
	WalUsage   *walusage;
	BufferUsage *bufferusage;
} BTLeader;
```
## Detailed Description
BTLeader serves as the coordinator structure for the leader process in parallel B-tree index construction. It maintains references to the parallel execution context and provides convenient access to shared state that would otherwise require TOC (Table of Contents) lookups. The leader process uses this structure to manage worker coordination and track resource usage during the parallel build process.

The structure includes pointers to both primary and secondary shared tuplesort states, with the secondary state used specifically for unique index builds. It also maintains tracking information for WAL and buffer usage statistics across all participants.

## Parameters / Member Variables
- `*pcxt`: Pointer to the parallel context managing the parallel execution
- `nparticipanttuplesorts`: Exact number of worker processes successfully launched, plus one if the leader participates as a worker (excludes leader in DISABLE_LEADER_PARTICIPATION builds)
- `*btshared`: Pointer to the BTShared structure containing shared state for the entire build
- `*sharedsort`: Pointer to the shared tuplesort-managed state passed to each process tuplesort
- `*sharedsort2`: Pointer to the corresponding btspool2 shared state, used only when building unique indexes
- `snapshot`: The snapshot used by the scan if an MVCC snapshot is required
- `*walusage`: Pointer to WAL usage statistics tracking structure
- `*bufferusage`: Pointer to buffer usage statistics tracking structure
## Dependencies
- Functions called/Symbols referenced:
  - [ParallelContext](../P/ParallelContext.md)
  - [BTShared](BTShared.md)
  - Sharedsort
  - WalUsage
  - BufferUsage
- Called from (representative examples):
  - [BTBuildState](BTBuildState.md)
  - [_bt_begin_parallel](../b/_bt_begin_parallel.md)
  - [_bt_end_parallel](../b/_bt_end_parallel.md)
  - [_bt_leader_participate_as_worker](../b/_bt_leader_participate_as_worker.md)

## Notes and Other Information
BTLeader is specifically designed for the leader process in parallel index builds and provides convenience pointers to avoid repeated TOC lookups during the build process. The nparticipanttuplesorts field accounts for both worker processes and the leader if it participates as a worker, with the leader participation controlled by the DISABLE_LEADER_PARTICIPATION build configuration. The structure maintains separate sharedsort and sharedsort2 pointers to handle the additional sorting requirements of unique indexes.