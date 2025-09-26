# ParallelHashJoinBatch

## Location
[src/include/executor/hashjoin.h:162-179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/hashjoin.h#L162-L179)

## Overview
ParallelHashJoinBatch is a shared memory coordination structure that manages individual batches in PostgreSQL's parallel hash join execution, providing synchronization and resource tracking for multiple worker processes operating on the same batch.

## Definition

```c
typedef struct ParallelHashJoinBatch
{
	dsa_pointer buckets;		/* array of hash table buckets */
	Barrier		batch_barrier;	/* synchronization for joining this batch */

	dsa_pointer chunks;			/* chunks of tuples loaded */
	size_t		size;			/* size of buckets + chunks in memory */
	size_t		estimated_size; /* size of buckets + chunks while writing */
	size_t		ntuples;		/* number of tuples loaded */
	size_t		old_ntuples;	/* number of tuples before repartitioning */
	bool		space_exhausted;
	bool		skip_unmatched; /* whether to abandon unmatched scan */

	/*
	 * Variable-sized SharedTuplestore objects follow this struct in memory.
	 * See the accessor macros below.
	 */
} ParallelHashJoinBatch;
```
## Detailed Description
ParallelHashJoinBatch serves as the coordination hub for individual batch processing in parallel hash joins. When a hash join operation is divided into multiple batches (due to memory constraints), each batch requires coordination among the parallel worker processes that will build and probe the hash table for that batch.

The structure manages both the hash table data (buckets and memory chunks) and the synchronization mechanisms needed for parallel access. The batch_barrier ensures that all workers coordinate their activities - for example, all workers must finish building the hash table before any worker begins the probing phase.

The structure also tracks memory usage and tuple counts, both current and historical (before repartitioning), which is crucial for making dynamic decisions about whether to further increase the number of batches during execution. The space_exhausted flag indicates when memory pressure forces batch subdivision, while skip_unmatched optimizes performance by abandoning unmatched tuple scans when appropriate.

Variable-sized SharedTuplestore objects are allocated immediately after this structure in memory, accessible through specialized macros, providing efficient shared storage for batch data across all participating processes.

## Parameters / Member Variables
- : DSA pointer to the array of hash table buckets for this batch, shared across all worker processes
- : Synchronization barrier ensuring coordinated execution phases (build/probe) among all workers processing this batch
- : DSA pointer to the linked list of memory chunks containing the actual tuple data for this batch
- : Current total memory consumption in bytes for both buckets and chunks
- : Estimated memory size during the writing/building phase, used for capacity planning
- : Current count of tuples loaded into this batch
- : Historical tuple count before any repartitioning operations, used for performance analysis
- : Boolean flag indicating whether this batch has exceeded available memory limits
- : Boolean flag controlling whether to skip the unmatched tuple scan phase for optimization

## Dependencies
- Functions called/Symbols referenced:
  - dsa_pointer (for shared memory access to buckets and chunks)
  - Barrier (for worker process synchronization)
- Called from (representative examples):
  - ExecParallelHashIncreaseNumBatches (batch subdivision operations)
  - ExecParallelHashRepartitionRest (repartitioning coordination)
  - ExecParallelPrepHashTableForUnmatched (unmatched tuple processing setup)
  - ExecParallelHashJoinSetUpBatches (initial batch setup)
  - ExecParallelHashEnsureBatchAccessors (accessor management)
  - ExecParallelHashTableAlloc (batch memory allocation)
  - ExecHashTableDetachBatch (batch cleanup and detachment)
  - ParallelHashJoinBatchInner (accessor macro)
  - EstimateParallelHashJoinBatch (memory estimation macro)
  - NthParallelHashJoinBatch (batch indexing macro)
  - ParallelHashJoinBatchAccessor (accessor creation macro)

## Notes and Other Information
- Structures are allocated in contiguous shared memory but not accessed as a direct array due to variable-sized trailing data
- Each batch structure is followed by variable-sized SharedTuplestore objects that are accessed through specialized macros
- The barrier synchronization is critical for correctness in parallel execution - workers must coordinate build/probe phases
- Memory size tracking (size vs estimated_size) enables dynamic batch count adjustment during execution
- The old_ntuples field supports performance analysis and optimization decisions during repartitioning
- Batch structures persist for the duration of the entire hash join operation, spanning multiple execution phases
- Used exclusively in parallel hash join scenarios; serial hash joins use simpler batch management structures