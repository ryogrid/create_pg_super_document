# ParallelHashJoinBatchAccessor

## Location
src/include/executor/hashjoin.h: 207 - 222

## Overview
ParallelHashJoinBatchAccessor is a structure that provides per-backend state for interacting with shared ParallelHashJoinBatch objects in parallel hash joins, maintaining local counters and state to reduce contention while coordinating batch processing.

## Definition

```c
typedef struct ParallelHashJoinBatchAccessor
{
	ParallelHashJoinBatch *shared;	/* pointer to shared state */

	/* Per-backend partial counters to reduce contention. */
	size_t		preallocated;	/* pre-allocated space for this backend */
	size_t		ntuples;		/* number of tuples */
	size_t		size;			/* size of partition in memory */
	size_t		estimated_size; /* size of partition on disk */
	size_t		old_ntuples;	/* how many tuples before repartitioning? */
	bool		at_least_one_chunk; /* has this backend allocated a chunk? */
	bool		outer_eof;		/* has this process hit end of batch? */
	bool		done;			/* flag to remember that a batch is done */
	SharedTuplestoreAccessor *inner_tuples;
	SharedTuplestoreAccessor *outer_tuples;
} ParallelHashJoinBatchAccessor;
```
## Detailed Description
ParallelHashJoinBatchAccessor serves as a per-backend interface to shared parallel hash join batch data. In parallel hash joins, multiple worker processes need to coordinate access to shared batches of data while maintaining their own local state. This structure provides that coordination by maintaining both a pointer to shared batch state and local counters that track this backend's contribution to the batch.

The accessor maintains separate counters for memory usage, tuple counts, and processing state to minimize contention on shared memory structures. Each backend can track its own progress through a batch (via outer_eof and done flags) and maintain its own tuple store accessors for reading inner and outer relation data.

The design follows a pattern where shared state is minimized and contention is reduced by maintaining per-backend copies of frequently updated counters. These local counters are periodically merged back to the shared state when coordination is required.

## Parameters / Member Variables
- : Pointer to the shared ParallelHashJoinBatch structure that coordinates all backends working on this batch
- : Amount of memory space this backend has pre-allocated for the batch to reduce future allocation overhead
- : Local count of tuples this backend has processed for this batch
- : Current memory size of the partition as seen by this backend
- : Estimated disk size of the partition, used for memory management decisions
- : Number of tuples that existed before any repartitioning operations
- : Boolean flag indicating whether this backend has allocated at least one chunk of memory
- : Boolean flag indicating whether this backend has reached the end of the outer relation for this batch
- : Boolean flag marking that this backend has completed processing this batch
- : Accessor for reading tuples from the inner relation's shared tuple store
- : Accessor for reading tuples from the outer relation's shared tuple store

## Dependencies
- Functions called/Symbols referenced:
  - ParallelHashJoinBatch
  - SharedTuplestoreAccessor
- Called from (representative examples):
  - ExecParallelHashMergeCounters
  - ExecParallelHashJoinSetUpBatches
  - ExecParallelHashEnsureBatchAccessors
  - ExecParallelHashTuplePrealloc
  - HashJoinTableData (as member)

## Notes and Other Information
This structure is a key component of PostgreSQL's parallel hash join implementation, which allows multiple worker processes to cooperatively build and probe hash tables. The accessor pattern reduces contention by maintaining per-backend state while providing controlled access to shared resources. The structure is typically allocated as an array within HashJoinTableData to provide one accessor per batch per backend.