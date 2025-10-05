# ExecParallelHashRepartitionFirst

## Location
[src/backend/executor/nodeHash.c:1312-1378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L1312-L1378)

## Overview
ExecParallelHashRepartitionFirst repartitions tuples currently loaded in memory for inner batch 0 when the number of batches has been increased, redistributing tuples between memory and disk-based later batches.

## Definition

```c
static void
ExecParallelHashRepartitionFirst(HashJoinTable hashtable)
```
## Detailed Description
This function performs the first phase of tuple repartitioning when the parallel hash join needs to increase the number of batches due to memory pressure. It processes all tuples currently residing in memory chunks for batch 0, recalculating their target batch numbers based on the new batch count, and either keeping them in memory (if they still belong to batch 0) or writing them to disk (if they now belong to a later batch).

The function works by:
1. Popping memory chunks from the shared work queue
2. Iterating through all tuples in each chunk
3. Recalculating bucket and batch assignments using the current hash values
4. For tuples remaining in batch 0: allocating new memory and copying the tuple
5. For tuples moving to later batches: writing them to the appropriate temporary storage
6. Updating batch statistics and freeing processed chunks

This repartitioning is essential for maintaining optimal memory usage and ensuring that the hash join can complete successfully even with large datasets.

## Parameters / Member Variables
- `hashtable`: The HashJoinTable containing the parallel hash join state, batch information, and memory chunks to be repartitioned
## Dependencies
- Functions called/Symbols referenced:
  - [ExecParallelHashPopChunkQueue](ExecParallelHashPopChunkQueue.md) (chunk queue management)
  - [ExecHashGetBucketAndBatch](ExecHashGetBucketAndBatch.md) (hash-to-bucket/batch mapping)
  - [ExecParallelHashTupleAlloc](ExecParallelHashTupleAlloc.md) (parallel tuple allocation)
  - [ExecParallelHashPushTuple](ExecParallelHashPushTuple.md) (tuple insertion into buckets)
  - [sts_puttuple](../s/sts_puttuple.md) (tuple storage to disk)
  - [dsa_free](../d/dsa_free.md) (shared memory deallocation)
  - HASH_CHUNK_DATA, HJTUPLE_MINTUPLE, HJTUPLE_OVERHEAD (tuple access macros)
  - CHECK_FOR_INTERRUPTS (interruption handling)

- Called from (representative examples):
  - [ExecParallelHashIncreaseNumBatches](ExecParallelHashIncreaseNumBatches.md)

## Notes and Other Information
- This function only handles the repartitioning of tuples that were already in memory for batch 0
- It maintains tuple counts for both the old batch 0 (old_ntuples) and the new target batches (ntuples)
- Tuples that remain in batch 0 are copied to new memory locations, while those moving to later batches are written to temporary storage
- The function processes all chunks in the shared work queue, which were populated during the batch increase operation
- Memory chunks are freed immediately after processing to avoid memory leaks
- The function includes interrupt checking to allow for query cancellation during long repartitioning operations

## Simplified Source

```c
static void
ExecParallelHashRepartitionFirst(HashJoinTable hashtable)
{
    dsa_pointer chunk_shared;
    HashMemoryChunk chunk;

    // Process all memory chunks from the shared queue
    while ((chunk = ExecParallelHashPopChunkQueue(hashtable, &chunk_shared))) {
        size_t idx = 0;

        // Repartition all tuples in this chunk
        while (idx < chunk->used) {
            HashJoinTuple hashTuple = (HashJoinTuple) (HASH_CHUNK_DATA(chunk) + idx);
            MinimalTuple tuple = HJTUPLE_MINTUPLE(hashTuple);
            int bucketno, batchno;

            // Recalculate batch assignment with new batch count
            ExecHashGetBucketAndBatch(hashtable, hashTuple->hashvalue,
                                     &bucketno, &batchno);

            if (batchno == 0) {
                // Tuple stays in batch 0 - copy to new memory location
                HashJoinTuple copyTuple;
                dsa_pointer shared;

                copyTuple = ExecParallelHashTupleAlloc(hashtable,
                                                      HJTUPLE_OVERHEAD + tuple->t_len,
                                                      &shared);
                copyTuple->hashvalue = hashTuple->hashvalue;
                memcpy(HJTUPLE_MINTUPLE(copyTuple), tuple, tuple->t_len);
                ExecParallelHashPushTuple(&hashtable->buckets.shared[bucketno],
                                         copyTuple, shared);
            }
            else {
                // Tuple moves to later batch - write to disk storage
                size_t tuple_size = MAXALIGN(HJTUPLE_OVERHEAD + tuple->t_len);
                hashtable->batches[batchno].estimated_size += tuple_size;
                sts_puttuple(hashtable->batches[batchno].inner_tuples,
                           &hashTuple->hashvalue, tuple);
            }

            // Update tuple counts
            ++hashtable->batches[0].old_ntuples;
            ++hashtable->batches[batchno].ntuples;

            idx += MAXALIGN(HJTUPLE_OVERHEAD + HJTUPLE_MINTUPLE(hashTuple)->t_len);
        }

        // Free processed chunk
        dsa_free(hashtable->area, chunk_shared);
        CHECK_FOR_INTERRUPTS();
    }
}
```