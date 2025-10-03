# ExecParallelHashIncreaseNumBuckets

## Location
[src/backend/executor/nodeHash.c:1532-1630](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L1532-L1630)

## Overview
Coordinates bucket array expansion in parallel hash joins by doubling the number of buckets and redistributing existing tuples across the new bucket structure using barrier synchronization.

## Definition

```c
static void
ExecParallelHashIncreaseNumBuckets(HashJoinTable hashtable)
```
## Detailed Description
This function implements a three-phase coordinated bucket expansion for parallel hash joins. When the hash table becomes too full, this function doubles the number of buckets to maintain efficient performance. The operation uses barrier synchronization to coordinate multiple worker processes through three distinct phases:

1. **PHJ_GROW_BUCKETS_ELECT**: One worker is elected to double the bucket array size and prepare the reallocation
2. **PHJ_GROW_BUCKETS_REALLOCATE**: All workers wait for the bucket array expansion to complete
3. **PHJ_GROW_BUCKETS_REINSERT**: All workers cooperate to redistribute existing tuples into the new bucket structure

The function handles dynamic shared memory allocation, maintains data consistency across parallel workers, and ensures that new participants joining during the operation can synchronize properly.

## Parameters / Member Variables
- : The HashJoinTable containing the parallel hash table state and bucket information to be expanded

## Dependencies
- Functions called/Symbols referenced:
  - [BarrierPhase](../B/BarrierPhase.md)
  - [BarrierArriveAndWait](../B/BarrierArriveAndWait.md)
  - [dsa_free](../d/dsa_free.md)
  - dsa_allocate
  - [dsa_get_address](../d/dsa_get_address.md)
  - dsa_pointer_atomic_init
  - [ExecParallelHashEnsureBatchAccessors](ExecParallelHashEnsureBatchAccessors.md)
  - [ExecParallelHashTableSetCurrentBatch](ExecParallelHashTableSetCurrentBatch.md)
  - [ExecParallelHashPopChunkQueue](ExecParallelHashPopChunkQueue.md)
  - [ExecHashGetBucketAndBatch](ExecHashGetBucketAndBatch.md)
  - [ExecParallelHashPushTuple](ExecParallelHashPushTuple.md)
- Called from (representative examples):
  - [MultiExecParallelHash](../M/MultiExecParallelHash.md)
  - [ExecParallelHashTupleAlloc](ExecParallelHashTupleAlloc.md)
  - [ExecParallelHashTuplePrealloc](ExecParallelHashTuplePrealloc.md)

## Notes and Other Information
- The function uses a three-phase barrier protocol to ensure consistent state across all parallel workers
- Only one worker (the elected one) performs the actual bucket array reallocation to avoid race conditions
- All workers participate in the tuple redistribution phase to maximize parallelism
- The function handles the case where new workers join during the expansion operation
- Memory management uses PostgreSQL's dynamic shared memory allocator (DSA)
- The operation is interruptible during the redistribution phase via CHECK_FOR_INTERRUPTS()

## Simplified Source

```c
static void
ExecParallelHashIncreaseNumBuckets(HashJoinTable hashtable)
{
    ParallelHashJoinState *pstate = hashtable->parallel_state;
    HashMemoryChunk chunk;
    dsa_pointer chunk_s;

    // Three-phase coordinated bucket expansion using barrier synchronization
    switch (PHJ_GROW_BUCKETS_PHASE(BarrierPhase(&pstate->grow_buckets_barrier)))
    {
        case PHJ_GROW_BUCKETS_ELECT:
            // Elect one worker to double bucket array size
            if (BarrierArriveAndWait(&pstate->grow_buckets_barrier,
                                   WAIT_EVENT_HASH_GROW_BUCKETS_ELECT))
            {
                // Double bucket count and reallocate shared memory
                pstate->nbuckets *= 2;
                size_t size = pstate->nbuckets * sizeof(dsa_pointer_atomic);

                // Free old buckets and allocate new larger array
                dsa_free(hashtable->area, hashtable->batches[0].shared->buckets);
                hashtable->batches[0].shared->buckets = dsa_allocate(hashtable->area, size);

                // Initialize all new bucket pointers to invalid
                dsa_pointer_atomic *buckets = (dsa_pointer_atomic *)
                    dsa_get_address(hashtable->area, hashtable->batches[0].shared->buckets);
                for (int i = 0; i < pstate->nbuckets; ++i)
                    dsa_pointer_atomic_init(&buckets[i], InvalidDsaPointer);

                // Queue existing chunks for redistribution
                pstate->chunk_work_queue = hashtable->batches[0].shared->chunks;
                pstate->growth = PHJ_GROWTH_OK;
            }
            /* Fall through */

        case PHJ_GROW_BUCKETS_REALLOCATE:
            // Wait for bucket reallocation to complete
            BarrierArriveAndWait(&pstate->grow_buckets_barrier,
                               WAIT_EVENT_HASH_GROW_BUCKETS_REALLOCATE);
            /* Fall through */

        case PHJ_GROW_BUCKETS_REINSERT:
            // All workers cooperate to redistribute tuples to new buckets
            ExecParallelHashEnsureBatchAccessors(hashtable);
            ExecParallelHashTableSetCurrentBatch(hashtable, 0);

            // Process each chunk from the work queue
            while ((chunk = ExecParallelHashPopChunkQueue(hashtable, &chunk_s)))
            {
                size_t idx = 0;

                // Redistribute each tuple in the chunk
                while (idx < chunk->used)
                {
                    HashJoinTuple hashTuple = (HashJoinTuple)(HASH_CHUNK_DATA(chunk) + idx);
                    dsa_pointer shared = chunk_s + HASH_CHUNK_HEADER_SIZE + idx;
                    int bucketno, batchno;

                    // Calculate new bucket for this tuple
                    ExecHashGetBucketAndBatch(hashtable, hashTuple->hashvalue,
                                            &bucketno, &batchno);

                    // Insert tuple into its new bucket
                    ExecParallelHashPushTuple(&hashtable->buckets.shared[bucketno],
                                            hashTuple, shared);

                    // Move to next tuple in chunk
                    idx += MAXALIGN(HJTUPLE_OVERHEAD +
                                  HJTUPLE_MINTUPLE(hashTuple)->t_len);
                }

                CHECK_FOR_INTERRUPTS(); // Allow cancellation
            }

            // Wait for all workers to complete redistribution
            BarrierArriveAndWait(&pstate->grow_buckets_barrier,
                               WAIT_EVENT_HASH_GROW_BUCKETS_REINSERT);
    }
}
```