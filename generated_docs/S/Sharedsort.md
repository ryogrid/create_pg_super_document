# Sharedsort

## Location
[src/backend/utils/sort/tuplesort.c:346-377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L346-L377)

## Overview
Sharedsort is the shared memory coordination structure for PostgreSQL's parallel tuple sorting operations, managing worker synchronization, file sharing, and tape allocation across multiple parallel sorting processes.

## Definition

```c
struct Sharedsort
{
	/* mutex protects all fields prior to tapes */
	slock_t		mutex;

	/*
	 * currentWorker generates ordinal identifier numbers for parallel sort
	 * workers.  These start from 0, and are always gapless.
	 *
	 * Workers increment workersFinished to indicate having finished.  If this
	 * is equal to state.nParticipants within the leader, leader is ready to
	 * merge worker runs.
	 */
	int			currentWorker;
	int			workersFinished;

	/* Temporary file space */
	SharedFileSet fileset;

	/* Size of tapes flexible array */
	int			nTapes;

	/*
	 * Tapes array used by workers to report back information needed by the
	 * leader to concatenate all worker tapes into one for merging
	 */
	TapeShare	tapes[FLEXIBLE_ARRAY_MEMBER];
};
```
## Detailed Description
Sharedsort serves as the central coordination point for parallel sorting operations in PostgreSQL. It is allocated in shared memory and provides the necessary synchronization mechanisms for multiple worker processes to collaborate on a single large sort operation.

The structure manages the parallel sorting lifecycle from worker registration through completion signaling. Workers obtain unique identifiers through the currentWorker counter, perform their sorting work independently, and signal completion through workersFinished. The leader process monitors this completion counter to determine when all workers have finished and it's time to merge the individual sorted runs.

The SharedFileSet provides a unified temporary file space that all workers can access, enabling them to write their sorted runs to shared storage. The flexible array of TapeShare structures allows workers to communicate metadata about their output tapes back to the leader, including tape positioning and run information needed for the final merge phase.

## Parameters / Member Variables
- : Spinlock protecting all shared fields before the tapes array
- : Counter generating unique ordinal identifiers for workers (starting from 0, gapless sequence)
- : Count of workers that have completed their sorting phase
- : Shared temporary file space accessible by all parallel workers
- : Size of the flexible tapes array
- : Flexible array of TapeShare structures containing worker tape metadata

## Dependencies
- Functions called/Symbols referenced:
  - slock_t (spinlock primitive)
  - SharedFileSet (shared temporary file management)
  - TapeShare (tape metadata sharing structure)
  - FLEXIBLE_ARRAY_MEMBER (C99 flexible array member)

- Called from (representative examples):
  - tuplesort_estimate_shared (memory size estimation)
  - tuplesort_initialize_shared (initialization)
  - tuplesort_attach_shared (worker attachment)
  - worker_get_identifier (worker ID assignment)
  - worker_freeze_result_tape (worker completion)
  - leader_takeover_tapes (leader merge preparation)
  - _bt_begin_parallel (B-tree index parallel building)
  - _brin_begin_parallel (BRIN index parallel building)

## Notes and Other Information
- The structure is allocated in dynamic shared memory to enable cross-process access
- The mutex provides fine-grained synchronization, protecting shared counters and state
- Worker identification follows a gapless sequence starting from 0 for predictable indexing
- The completion signaling mechanism (workersFinished vs nParticipants) enables deterministic merge triggering
- TapeShare structures in the flexible array contain essential metadata for tape concatenation and merging
- Used extensively in parallel index building operations for B-tree and BRIN indexes
- The design supports variable numbers of workers through the flexible array member pattern
- SharedFileSet integration ensures all workers can access the same temporary file space regardless of process boundaries
- Critical for scaling sort operations across multiple CPU cores in large dataset scenarios