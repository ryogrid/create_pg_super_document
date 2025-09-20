# BrinLeader

## Location
[src/backend/access/brin/brin.c:119-146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L119-L146)

## Overview
BrinLeader is a structure that manages the leader process state during parallel BRIN index builds, providing convenience pointers to shared state and coordinating worker processes.

## Definition

```c
typedef struct BrinLeader
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
	 * brinshared is the shared state for entire build.  sharedsort is the
	 * shared, tuplesort-managed state passed to each process tuplesort.
	 * snapshot is the snapshot used by the scan iff an MVCC snapshot is
	 * required.
	 */
	BrinShared *brinshared;
	Sharedsort *sharedsort;
	Snapshot	snapshot;
	WalUsage   *walusage;
	BufferUsage *bufferusage;
} BrinLeader;
```
## Detailed Description
BrinLeader serves as the control structure for the leader process in parallel BRIN index builds. It maintains references to the parallel execution context and provides convenient access to shared state that would otherwise require table of contents (TOC) lookups. The leader process uses this structure to coordinate with worker processes and manage the overall parallel build operation.

The structure includes pointers to shared memory segments for coordination, performance tracking (WAL and buffer usage), and snapshot management for MVCC consistency when required.

## Parameters / Member Variables
- : Pointer to the parallel execution context managing the parallel build
- : Total number of participant processes including successfully launched workers plus the leader if it participates as a worker
- : Convenience pointer to the shared state structure for the entire build process
- : Pointer to shared tuplesort-managed state passed to each process
- : MVCC snapshot used by the scan when MVCC snapshot is required
- : Pointer to WAL usage statistics for performance tracking
- : Pointer to buffer usage statistics for performance monitoring

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelContext](../P/ParallelContext.md)
  - [BrinShared](BrinShared.md)
  - Sharedsort
  - WalUsage
  - BufferUsage
- Called from (representative examples):
  - [BrinBuildState](BrinBuildState.md)
  - [_brin_begin_parallel](../b/_brin_begin_parallel.md)
  - [_brin_end_parallel](../b/_brin_end_parallel.md)
  - [_brin_leader_participate_as_worker](../b/_brin_leader_participate_as_worker.md)

## Notes and Other Information
The leader avoids TOC lookups by maintaining direct pointers to shared state. The nparticipanttuplesorts count excludes builds with DISABLE_LEADER_PARTICIPATION where the leader does not participate as a worker. The structure is essential for coordinating the parallel build process and collecting performance statistics.