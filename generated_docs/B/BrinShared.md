# BrinShared

## Location
[src/backend/access/brin/brin.c:57-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L57-L105)

## Overview
BrinShared is a structure that stores status information for BRIN index builds performed in parallel, allocated in a dynamic shared memory segment to coordinate between leader and worker processes.

## Definition

```c
typedef struct BrinShared
{
	/*
	 * These fields are not modified during the build.  They primarily exist
	 * for the benefit of worker processes that need to create state
	 * corresponding to that used by the leader.
	 */
	Oid			heaprelid;
	Oid			indexrelid;
	bool		isconcurrent;
	BlockNumber pagesPerRange;
	int			scantuplesortstates;

	/*
	 * workersdonecv is used to monitor the progress of workers.  All parallel
	 * participants must indicate that they are done before leader can use
	 * results built by the workers (and before leader can write the data into
	 * the index).
	 */
	ConditionVariable workersdonecv;

	/*
	 * mutex protects all fields before heapdesc.
	 *
	 * These fields contain status information of interest to BRIN index
	 * builds that must work just the same when an index is built in parallel.
	 */
	slock_t		mutex;

	/*
	 * Mutable state that is maintained by workers, and reported back to
	 * leader at end of the scans.
	 *
	 * nparticipantsdone is number of worker processes finished.
	 *
	 * reltuples is the total number of input heap tuples.
	 *
	 * indtuples is the total number of tuples that made it into the index.
	 */
	int			nparticipantsdone;
	double		reltuples;
	double		indtuples;

	/*
	 * ParallelTableScanDescData data follows. Can't directly embed here, as
	 * implementations of the parallel table scan desc interface might need
	 * stronger alignment.
	 */
} BrinShared;
```
## Detailed Description
BrinShared serves as the central coordination structure for parallel BRIN index builds. It contains both immutable configuration data that worker processes need to replicate the leader's state, and mutable status fields that track the progress of the parallel build operation. The structure is designed to be allocated in dynamic shared memory, allowing multiple processes to coordinate their work on building a BRIN index.

The structure includes a condition variable for monitoring worker progress and a mutex for protecting shared state updates. Workers report their completion status and tuple counts back to the leader through this shared structure.

## Parameters / Member Variables
- : OID of the heap relation being indexed
- : OID of the BRIN index being built
- : Flag indicating whether this is a concurrent index build
- : Number of pages per BRIN range for this index
- : Number of scan tuple sort states
- : Condition variable used to monitor worker process completion
- : Spinlock protecting mutable fields in the structure
- : Number of worker processes that have finished their work
- : Total number of input heap tuples processed
- : Total number of tuples that were successfully inserted into the index

## Dependencies
- Functions called/Symbols referenced:
  - ConditionVariable
  - [slock_t](../s/slock_t.md)
- Called from (representative examples):
  - ParallelTableScanFromBrinShared
  - [BrinLeader](BrinLeader.md)
  - [_brin_begin_parallel](../b/_brin_begin_parallel.md)
  - [_brin_parallel_heapscan](../b/_brin_parallel_heapscan.md)
  - [_brin_parallel_estimate_shared](../b/_brin_parallel_estimate_shared.md)

## Notes and Other Information
The structure is followed by ParallelTableScanDescData which cannot be directly embedded due to potential alignment requirements. The mutex protects all mutable fields, ensuring thread-safe updates to progress tracking information. The condition variable allows the leader to efficiently wait for all workers to complete before proceeding with index finalization.