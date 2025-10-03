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
  - [ParallelHashJoinState](../P/ParallelHashJoinState.md) (parallel coordination state)
  - [HashJoinTable](../H/HashJoinTable.md) (shared hash table structure)
  - [Barrier](../B/Barrier.md) (synchronization primitive)
  - outerPlanState (child plan access)
  - PHJ_BUILD_ALLOCATE, PHJ_BUILD_HASH_INNER, PHJ_BUILD_FREE (phase constants)
  - [BarrierPhase](../B/BarrierPhase.md), BarrierArriveAndWait, BarrierAttach, BarrierDetach (barrier operations)
  - PHJ_GROW_BATCHES_PHASE, PHJ_GROW_BUCKETS_PHASE (growth phase checks)
  - [ExecParallelHashIncreaseNumBatches](../E/ExecParallelHashIncreaseNumBatches.md), ExecParallelHashIncreaseNumBuckets (parallel resizing)
  - [ExecParallelHashEnsureBatchAccessors](../E/ExecParallelHashEnsureBatchAccessors.md) (batch file management)
  - [ExecParallelHashTableSetCurrentBatch](../E/ExecParallelHashTableSetCurrentBatch.md) (batch switching)
  - [ExecProcNode](../E/ExecProcNode.md), TupIsNull (tuple processing)
  - [ExecHashGetHashValue](../E/ExecHashGetHashValue.md) (hash computation)
  - [ExecParallelHashTableInsert](../E/ExecParallelHashTableInsert.md) (parallel-safe insertion)
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

## Simplified Source

```c
static void MultiExecParallelHash(HashState *node)
{
    ParallelHashJoinState *pstate;
    PlanState *outerNode;
    List *hashkeys;
    HashJoinTable hashtable;
    TupleTableSlot *slot;
    ExprContext *econtext;
    uint32 hashvalue;
    Barrier *build_barrier;

    // Initialize state from node
    outerNode = outerPlanState(node);
    hashtable = node->hashtable;
    hashkeys = node->hashkeys;
    econtext = node->ps.ps_ExprContext;

    // Get synchronization barrier
    pstate = hashtable->parallel_state;
    build_barrier = &pstate->build_barrier;

    // Synchronize through build phases
    switch (BarrierPhase(build_barrier)) {
        case PHJ_BUILD_ALLOCATE:
            // Wait for hash table allocation
            BarrierArriveAndWait(build_barrier, WAIT_EVENT_HASH_BUILD_ALLOCATE);
            // Fall through

        case PHJ_BUILD_HASH_INNER:
            // Handle parallel growth if needed
            if (PHJ_GROW_BATCHES_PHASE(BarrierAttach(&pstate->grow_batches_barrier)) != PHJ_GROW_BATCHES_ELECT)
                ExecParallelHashIncreaseNumBatches(hashtable);
            if (PHJ_GROW_BUCKETS_PHASE(BarrierAttach(&pstate->grow_buckets_barrier)) != PHJ_GROW_BUCKETS_ELECT)
                ExecParallelHashIncreaseNumBuckets(hashtable);

            // Setup batch accessors and process tuples
            ExecParallelHashEnsureBatchAccessors(hashtable);
            ExecParallelHashTableSetCurrentBatch(hashtable, 0);

            // Process all input tuples
            for (;;) {
                slot = ExecProcNode(outerNode);
                if (TupIsNull(slot))
                    break;
                econtext->ecxt_outertuple = slot;
                if (ExecHashGetHashValue(hashtable, econtext, hashkeys, false, hashtable->keepNulls, &hashvalue))
                    ExecParallelHashTableInsert(hashtable, slot, hashvalue);
                hashtable->partialTuples++;
            }

            // Finalize batch files and merge counters
            for (int i = 0; i < hashtable->nbatch; ++i)
                sts_end_write(hashtable->batches[i].inner_tuples);
            ExecParallelHashMergeCounters(hashtable);

            // Cleanup barriers and wait for completion
            BarrierDetach(&pstate->grow_buckets_barrier);
            BarrierDetach(&pstate->grow_batches_barrier);
            if (BarrierArriveAndWait(build_barrier, WAIT_EVENT_HASH_BUILD_HASH_INNER))
                pstate->growth = PHJ_GROWTH_DISABLED;
    }

    // Setup final hash table state
    hashtable->curbatch = -1;
    hashtable->nbuckets = pstate->nbuckets;
    hashtable->log2_nbuckets = my_log2(hashtable->nbuckets);
    hashtable->totalTuples = pstate->total_tuples;

    // Ensure batch accessors are available
    if (BarrierPhase(build_barrier) < PHJ_BUILD_FREE)
        ExecParallelHashEnsureBatchAccessors(hashtable);
}
```