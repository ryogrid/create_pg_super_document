# MultiExecParallelHash

## Location
[src/backend/executor/nodeHash.c:214-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L214-L359)

## Overview
MultiExecParallelHash implements the parallel-aware hash table building algorithm, coordinating multiple backend processes to build a shared hash table using barriers and phases for synchronization.

## Definition

```c
structure
	 */
	hashstate = makeNode(HashState);
```
## Detailed Description
MultiExecParallelHash orchestrates the parallel construction of hash tables across multiple cooperating backend processes. It implements a sophisticated multi-phase algorithm using barriers to synchronize parallel workers through different stages of hash table construction.

The function operates through distinct phases: allocation (PHJ_BUILD_ALLOCATE), inner table hashing (PHJ_BUILD_HASH_INNER), and coordination phases. During execution, it must handle dynamic resizing of both batches and buckets while multiple processes are simultaneously inserting tuples, requiring careful coordination to maintain consistency.

Key features include handling parallel batch and bucket growth through specialized barriers, ensuring tuple visibility across processes using shared tuple storage (sts_end_write), and merging counters from all participating workers to maintain accurate statistics for query optimization.

The algorithm is designed to scale efficiently across multiple CPU cores while maintaining data integrity and optimal hash table performance characteristics.

## Parameters / Member Variables
- : HashState containing parallel execution state, shared hash table, and synchronization structures

## Dependencies
- Functions called/Symbols referenced:
  - [HashState](../H/HashState.md) (parameter and state references)
  - ParallelHashJoinState (parallel coordination state)
  - [HashJoinTable](../H/HashJoinTable.md) (shared hash table structure)
  - [Barrier](../B/Barrier.md) (synchronization primitive)
  - outerPlanState (child plan access)
  - PHJ_BUILD_ALLOCATE, PHJ_BUILD_HASH_INNER, PHJ_BUILD_FREE (phase constants)
  - BarrierPhase, BarrierArriveAndWait, BarrierAttach, BarrierDetach (barrier operations)
  - PHJ_GROW_BATCHES_PHASE, PHJ_GROW_BUCKETS_PHASE (growth phase checks)
  - [ExecParallelHashIncreaseNumBatches](../E/ExecParallelHashIncreaseNumBatches.md), ExecParallelHashIncreaseNumBuckets (parallel resizing)
  - [ExecParallelHashEnsureBatchAccessors](../E/ExecParallelHashEnsureBatchAccessors.md) (batch file management)
  - [ExecParallelHashTableSetCurrentBatch](../E/ExecParallelHashTableSetCurrentBatch.md) (batch switching)
  - ExecProcNode, TupIsNull (tuple processing)
  - ExecHashGetHashValue (hash computation)
  - ExecParallelHashTableInsert (parallel-safe insertion)
  - [sts_end_write](../s/sts_end_write.md) (shared tuple storage finalization)
  - [ExecParallelHashMergeCounters](../E/ExecParallelHashMergeCounters.md) (statistics aggregation)
  - [my_log2](../m/my_log2.md) (logarithm utility)
- Called from (representative examples):
  - [MultiExecHash](MultiExecHash.md) (parallel execution path)

## Notes and Other Information
- Implements complex barrier-based synchronization to coordinate multiple parallel workers
- Handles dynamic growth of batches and buckets while parallel insertion is occurring
- Uses shared tuple storage (STS) for efficient cross-process data sharing
- Manages different build phases: ALLOCATE → HASH_INNER → HASH_OUTER → RUN → FREE
- Workers must coordinate to elect leaders for certain operations (batch/bucket growth, growth disabling)
- Merges tuple counters from all workers for accurate statistics and empty table optimization
- Ensures proper cleanup and resource management across all participating processes
- Located in src/backend/executor/nodeHash.c:214-359