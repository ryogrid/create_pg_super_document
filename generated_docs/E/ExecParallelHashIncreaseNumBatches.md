# ExecParallelHashIncreaseNumBatches

## Location
[src/backend/executor/nodeHash.c:1080-1311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L1080-L1311)

## Overview
ExecParallelHashIncreaseNumBatches coordinates the dynamic expansion of batch count across all parallel participants in a hash join operation when memory pressure requires repartitioning of data into more batches.

## Definition

```c
static void
ExecParallelHashIncreaseNumBatches(HashJoinTable hashtable)
```
## Detailed Description
This function orchestrates a complex multi-phase operation that increases the number of batches in a parallel hash join when the current batching scheme becomes insufficient due to memory constraints. The function implements a state machine with multiple barrier synchronization phases to ensure all parallel workers coordinate properly during the repartitioning process.

The operation involves several coordinated phases:
1. **Election Phase (PHJ_GROW_BATCHES_ELECT)**: One participant is elected to prepare the new batch structure
2. **Reallocation Phase (PHJ_GROW_BATCHES_REALLOCATE)**: Wait for structural changes to complete
3. **Repartitioning Phase (PHJ_GROW_BATCHES_REPARTITION)**: All participants repartition their data
4. **Decision Phase (PHJ_GROW_BATCHES_DECIDE)**: Evaluate whether further growth is needed or beneficial
5. **Finish Phase (PHJ_GROW_BATCHES_FINISH)**: Complete the operation

The function handles the transition from single-batch to multi-batch mode specially, adjusting memory budgets and calculating optimal batch counts. It also implements safeguards against extreme data skew and prevents unbounded growth.

## Parameters / Member Variables
- `hashtable`: The HashJoinTable structure containing the parallel hash join state and batch information
## Dependencies
- Functions called/Symbols referenced:
  - [BarrierPhase](../B/BarrierPhase.md), BarrierArriveAndWait (barrier synchronization)
  - [ExecParallelHashCloseBatchAccessors](ExecParallelHashCloseBatchAccessors.md), ExecParallelHashEnsureBatchAccessors (batch management)
  - [ExecParallelHashJoinSetUpBatches](ExecParallelHashJoinSetUpBatches.md) (batch setup)
  - [ExecParallelHashRepartitionFirst](ExecParallelHashRepartitionFirst.md), ExecParallelHashRepartitionRest (repartitioning)
  - [ExecParallelHashMergeCounters](ExecParallelHashMergeCounters.md) (counter management)
  - [ExecParallelHashTableSetCurrentBatch](ExecParallelHashTableSetCurrentBatch.md) (batch selection)
  - [get_hash_memory_limit](../g/get_hash_memory_limit.md) (memory management)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md), pg_prevpower2_32 (power-of-2 calculations)
  - dsa_allocate, dsa_free, dsa_get_address (shared memory management)
  - NthParallelHashJoinBatch (batch access)

- Called from (representative examples):
  - [MultiExecParallelHash](../M/MultiExecParallelHash.md)
  - [ExecParallelHashTupleAlloc](ExecParallelHashTupleAlloc.md)
  - [ExecParallelHashTuplePrealloc](ExecParallelHashTuplePrealloc.md)

## Notes and Other Information
- This function is critical for handling memory pressure in parallel hash joins by dynamically increasing the number of batches
- The multi-phase barrier synchronization ensures all parallel workers remain coordinated during the complex repartitioning operation
- Special handling exists for the transition from single-batch (unlimited memory) to multi-batch mode
- The function includes logic to detect and handle extreme data skew that would make further repartitioning ineffective
- Growth can be disabled if repartitioning is not helping or if the maximum number of batches would be exceeded
- The function operates on shared memory structures accessible by all parallel participants

## Simplified Source

```c
static void
ExecParallelHashIncreaseNumBatches(HashJoinTable hashtable)
{
    ParallelHashJoinState *pstate = hashtable->parallel_state;

    // Coordinate through multi-phase barrier synchronization
    switch (PHJ_GROW_BATCHES_PHASE(BarrierPhase(&pstate->grow_batches_barrier)))
    {
        case PHJ_GROW_BATCHES_ELECT:
            // Elect leader to prepare new batch structure
            if (BarrierArriveAndWait(&pstate->grow_batches_barrier, WAIT_EVENT_HASH_GROW_BATCHES_ELECT))
            {
                // Leader: save old batches and calculate new batch count
                ParallelHashJoinBatch *old_batch0 = hashtable->batches[0].shared;
                pstate->old_batches = pstate->batches;
                pstate->old_nbatch = hashtable->nbatch;

                ExecParallelHashCloseBatchAccessors(hashtable);

                // Determine new batch count
                int new_nbatch;
                if (hashtable->nbatch == 1)
                {
                    // Transition from single to multi-batch
                    pstate->space_allowed = get_hash_memory_limit();
                    new_nbatch = pg_nextpower2_32(pstate->nparticipants * 2);
                }
                else
                {
                    // Double existing batch count
                    new_nbatch = hashtable->nbatch * 2;
                }

                // Set up new larger batch generation
                ExecParallelHashJoinSetUpBatches(hashtable, new_nbatch);

                // Handle bucket array reallocation or recycling
                if (pstate->old_nbatch == 1)
                {
                    // Calculate smaller bucket array for transition case
                    double dtuples = (old_batch0->ntuples * 2.0) / new_nbatch;
                    int new_nbuckets = calculate_optimal_buckets(dtuples);

                    // Allocate new bucket array
                    dsa_free(hashtable->area, old_batch0->buckets);
                    hashtable->batches[0].shared->buckets =
                        dsa_allocate(hashtable->area, sizeof(dsa_pointer_atomic) * new_nbuckets);
                    initialize_buckets(hashtable, new_nbuckets);
                }
                else
                {
                    // Recycle existing bucket array
                    hashtable->batches[0].shared->buckets = old_batch0->buckets;
                    clear_buckets(hashtable);
                }

                // Queue old chunks for repartitioning
                pstate->chunk_work_queue = old_batch0->chunks;
                pstate->growth = PHJ_GROWTH_DISABLED; // Disable growth during repartition
            }
            else
            {
                ExecParallelHashCloseBatchAccessors(hashtable);
            }
            // Fall through

        case PHJ_GROW_BATCHES_REALLOCATE:
            BarrierArriveAndWait(&pstate->grow_batches_barrier, WAIT_EVENT_HASH_GROW_BATCHES_REALLOCATE);
            // Fall through

        case PHJ_GROW_BATCHES_REPARTITION:
            // All participants repartition data
            ExecParallelHashEnsureBatchAccessors(hashtable);
            ExecParallelHashTableSetCurrentBatch(hashtable, 0);
            ExecParallelHashRepartitionFirst(hashtable);
            ExecParallelHashRepartitionRest(hashtable);
            ExecParallelHashMergeCounters(hashtable);
            BarrierArriveAndWait(&pstate->grow_batches_barrier, WAIT_EVENT_HASH_GROW_BATCHES_REPARTITION);
            // Fall through

        case PHJ_GROW_BATCHES_DECIDE:
            // Elect leader to evaluate results and decide next steps
            if (BarrierArriveAndWait(&pstate->grow_batches_barrier, WAIT_EVENT_HASH_GROW_BATCHES_DECIDE))
            {
                ExecParallelHashEnsureBatchAccessors(hashtable);

                bool space_exhausted = false;
                bool extreme_skew_detected = false;

                // Check if any batches are still exhausted or show extreme skew
                for (int i = 0; i < hashtable->nbatch; ++i)
                {
                    ParallelHashJoinBatch *batch = hashtable->batches[i].shared;
                    if (batch->space_exhausted || batch->estimated_size > pstate->space_allowed)
                        space_exhausted = true;

                    // Check for extreme skew (all tuples went to one batch)
                    int parent = i % pstate->old_nbatch;
                    if (batch->ntuples == hashtable->batches[parent].shared->old_ntuples)
                        extreme_skew_detected = true;
                }

                // Decide on further growth
                if (extreme_skew_detected || hashtable->nbatch >= INT_MAX / 2)
                    pstate->growth = PHJ_GROWTH_DISABLED;
                else if (space_exhausted)
                    pstate->growth = PHJ_GROWTH_NEED_MORE_BATCHES;
                else
                    pstate->growth = PHJ_GROWTH_OK;

                // Clean up old batch structures
                dsa_free(hashtable->area, pstate->old_batches);
                pstate->old_batches = InvalidDsaPointer;
            }
            // Fall through

        case PHJ_GROW_BATCHES_FINISH:
            BarrierArriveAndWait(&pstate->grow_batches_barrier, WAIT_EVENT_HASH_GROW_BATCHES_FINISH);
    }
}
```

This simplified version shows the coordinated multi-phase batch expansion: leader election and setup, parallel repartitioning of data, and evaluation of results to determine if further growth is needed.