# ExecHashIncreaseNumBuckets

## Location
[src/backend/executor/nodeHash.c:1469-1531](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L1469-L1531)

## Overview
ExecHashIncreaseNumBuckets dynamically increases the number of buckets in a hash table to reduce the number of tuples per bucket, improving hash lookup performance when the optimal bucket count differs from the initial allocation.

## Definition

```c
static void
ExecHashIncreaseNumBuckets(HashJoinTable hashtable)
```
## Detailed Description
This function optimizes hash table performance by increasing the number of buckets when analysis indicates that a larger bucket array would improve lookup efficiency. Unlike batch repartitioning which addresses memory pressure, bucket increase addresses hash distribution efficiency by reducing the average chain length per bucket.

The function operates by:
1. Checking if an increase is actually needed (current < optimal)
2. Reallocating the bucket array to the optimal size
3. Clearing the new bucket array
4. Rebuilding the hash table by walking through all memory chunks
5. Recalculating bucket assignments for each tuple
6. Linking tuples into their new bucket chains

This reorganization maintains all tuples in memory while redistributing them across more buckets, resulting in shorter bucket chains and faster hash lookups. The operation is performed only in non-parallel hash joins since parallel hash joins use different memory management strategies.

## Parameters / Member Variables
- `hashtable`: The HashJoinTable containing the hash table structure, current bucket configuration, and memory chunks to be reorganized
## Dependencies
- Functions called/Symbols referenced:
  - repalloc_array (memory reallocation)
  - memset (memory initialization)
  - [ExecHashGetBucketAndBatch](ExecHashGetBucketAndBatch.md) (hash-to-bucket mapping)
  - HASH_CHUNK_DATA, HJTUPLE_MINTUPLE, HJTUPLE_OVERHEAD (tuple access macros)
  - CHECK_FOR_INTERRUPTS (interruption handling)
  - MAXALIGN (memory alignment)

- Called from (representative examples):
  - [MultiExecPrivateHash](../M/MultiExecPrivateHash.md)

## Notes and Other Information
- This function only operates on non-parallel hash joins (uses unshared bucket arrays and chunk pointers)
- The function includes early return if no increase is needed to avoid unnecessary work
- Debug output is available when HJDEBUG is defined to track bucket count changes
- The bucket count must be a power of 2, which is validated through assertions
- All tuples remain in memory during the reorganization, unlike batch repartitioning operations
- The function rebuilds bucket chains by walking through dense-allocated memory chunks rather than existing bucket chains
- Interrupt checking allows for query cancellation during potentially long reorganization operations
- This optimization improves hash table performance without changing the fundamental data or requiring disk I/O

## Simplified Source
```c
static void ExecHashIncreaseNumBuckets(HashJoinTable hashtable)
{
    // Skip if no increase needed
    if (hashtable->nbuckets >= hashtable->nbuckets_optimal)
        return;

    // Update to optimal bucket configuration
    hashtable->nbuckets = hashtable->nbuckets_optimal;
    hashtable->log2_nbuckets = hashtable->log2_nbuckets_optimal;

    // Reallocate and clear bucket array
    hashtable->buckets.unshared = repalloc_array(hashtable->buckets.unshared,
                                                 HashJoinTuple, hashtable->nbuckets);
    memset(hashtable->buckets.unshared, 0,
           hashtable->nbuckets * sizeof(HashJoinTuple));

    // Rebuild hash table by walking through all chunks
    for (HashMemoryChunk chunk = hashtable->chunks; chunk != NULL; chunk = chunk->next.unshared)
    {
        size_t idx = 0;

        // Process all tuples in this chunk
        while (idx < chunk->used)
        {
            HashJoinTuple hashTuple = (HashJoinTuple)(HASH_CHUNK_DATA(chunk) + idx);
            int bucketno, batchno;

            // Calculate new bucket for this tuple
            ExecHashGetBucketAndBatch(hashtable, hashTuple->hashvalue,
                                      &bucketno, &batchno);

            // Link tuple into new bucket chain
            hashTuple->next.unshared = hashtable->buckets.unshared[bucketno];
            hashtable->buckets.unshared[bucketno] = hashTuple;

            // Move to next tuple
            idx += MAXALIGN(HJTUPLE_OVERHEAD + HJTUPLE_MINTUPLE(hashTuple)->t_len);
        }

        CHECK_FOR_INTERRUPTS();
    }
}
```