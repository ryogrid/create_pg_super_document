# ExecHashIncreaseNumBatches

## Location
[src/backend/executor/nodeHash.c:916-1079](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L916-L1079)

## Overview
Dynamically doubles the number of batches in a hash table to reduce memory consumption by spilling approximately half of the current tuples to temporary files.

## Definition
static void ExecHashIncreaseNumBatches(HashJoinTable hashtable)

## Detailed Description
ExecHashIncreaseNumBatches implements PostgreSQL's dynamic memory management strategy for hash joins when memory consumption exceeds available limits. The function reorganizes the hash table by doubling the number of batches, which effectively redistributes tuples such that approximately half remain in memory while the other half are spilled to temporary batch files for later processing.

The function performs several critical operations: it doubles the batch count and enlarges or creates the batch file arrays if needed, scans through all existing hash table chunks to redistribute tuples based on their new batch assignments, keeps tuples belonging to the current batch in memory while spilling others to appropriate batch files, and optionally resizes the bucket array if an optimal size has been determined.

A key optimization is the ability to simultaneously resize the bucket array during rebatching, avoiding the need for a separate reorganization pass. The function also includes safeguards against pathological cases where all tuples have identical hash values, which would make further batching ineffective.

Memory management is carefully handled through proper context switching and chunk-by-chunk processing to avoid excessive memory usage during the reorganization process.

## Parameters / Member Variables
- hashtable: HashJoinTable structure containing the hash table state, batch configuration, memory chunks, and file arrays

## Dependencies
- Functions called/Symbols referenced:
  - palloc0_array/repalloc0_array (allocate/reallocate batch file arrays)
  - [PrepareTempTablespaces](../P/PrepareTempTablespaces.md) (ensure temporary tablespace availability)
  - [ExecHashGetBucketAndBatch](ExecHashGetBucketAndBatch.md) (determine new batch assignment for tuples)
  - [dense_alloc](../d/dense_alloc.md) (allocate space in hash table chunks)
  - [ExecHashJoinSaveTuple](ExecHashJoinSaveTuple.md) (save tuples to batch files)
  - CHECK_FOR_INTERRUPTS (allow query cancellation)
- Called from (representative examples):
  - [ExecHashTableInsert](ExecHashTableInsert.md) (when memory limit exceeded during insertion)
  - [ExecHashSkewTableInsert](ExecHashSkewTableInsert.md) (when skew table memory limit exceeded)

## Dependencies
- Functions called/Symbols referenced:
  - palloc0_array/repalloc0_array (allocate/reallocate batch file arrays)
  - [PrepareTempTablespaces](../P/PrepareTempTablespaces.md) (ensure temporary tablespace availability)
  - [ExecHashGetBucketAndBatch](ExecHashGetBucketAndBatch.md) (determine new batch assignment for tuples)
  - [dense_alloc](../d/dense_alloc.md) (allocate space in hash table chunks)
  - [ExecHashJoinSaveTuple](ExecHashJoinSaveTuple.md) (save tuples to batch files)
  - CHECK_FOR_INTERRUPTS (allow query cancellation)
- Called from (representative examples):
  - [ExecHashTableInsert](ExecHashTableInsert.md) (when memory limit exceeded during insertion)
  - [ExecHashSkewTableInsert](ExecHashSkewTableInsert.md) (when skew table memory limit exceeded)

## Notes and Other Information
- The function is static and only called internally when memory pressure is detected
- Batch count is always doubled to maintain power-of-2 sizing for efficient hash computation
- Growth can be permanently disabled if rebatching proves ineffective (all tuples have same hash value)
- File arrays are created lazily - first call allocates them, subsequent calls enlarge them
- The algorithm processes chunks directly rather than following bucket chains to ensure all tuples are handled exactly once
- Includes safety checks to prevent integer overflow in batch count calculations
- Bucket array resizing is performed opportunistically during rebatching for efficiency

## Simplified Source

```c
static void
ExecHashIncreaseNumBatches(HashJoinTable hashtable)
{
    int oldnbatch = hashtable->nbatch;
    int curbatch = hashtable->curbatch;
    int nbatch;
    long ninmemory, nfreed;
    HashMemoryChunk oldchunks;

    // Exit early if growth is disabled or would overflow
    if (!hashtable->growEnabled ||
        oldnbatch > Min(INT_MAX / 2, MaxAllocSize / (sizeof(void *) * 2)))
        return;

    // Double the number of batches
    nbatch = oldnbatch * 2;

    // Create or enlarge batch file arrays
    if (hashtable->innerBatchFile == NULL)
    {
        MemoryContext oldcxt = MemoryContextSwitchTo(hashtable->spillCxt);
        hashtable->innerBatchFile = palloc0_array(BufFile *, nbatch);
        hashtable->outerBatchFile = palloc0_array(BufFile *, nbatch);
        MemoryContextSwitchTo(oldcxt);
        PrepareTempTablespaces();
    }
    else
    {
        hashtable->innerBatchFile = repalloc0_array(hashtable->innerBatchFile,
                                                   BufFile *, oldnbatch, nbatch);
        hashtable->outerBatchFile = repalloc0_array(hashtable->outerBatchFile,
                                                   BufFile *, oldnbatch, nbatch);
    }

    hashtable->nbatch = nbatch;

    // Resize buckets if needed
    if (hashtable->nbuckets_optimal != hashtable->nbuckets)
    {
        hashtable->nbuckets = hashtable->nbuckets_optimal;
        hashtable->log2_nbuckets = hashtable->log2_nbuckets_optimal;
        hashtable->buckets.unshared = repalloc_array(hashtable->buckets.unshared,
                                                    HashJoinTuple, hashtable->nbuckets);
    }

    // Clear bucket array and save old chunks
    memset(hashtable->buckets.unshared, 0, sizeof(HashJoinTuple) * hashtable->nbuckets);
    oldchunks = hashtable->chunks;
    hashtable->chunks = NULL;
    ninmemory = nfreed = 0;

    // Process all tuples in old chunks
    while (oldchunks != NULL)
    {
        HashMemoryChunk nextchunk = oldchunks->next.unshared;
        size_t idx = 0;

        // Process each tuple in this chunk
        while (idx < oldchunks->used)
        {
            HashJoinTuple hashTuple = (HashJoinTuple) (HASH_CHUNK_DATA(oldchunks) + idx);
            MinimalTuple tuple = HJTUPLE_MINTUPLE(hashTuple);
            int hashTupleSize = (HJTUPLE_OVERHEAD + tuple->t_len);
            int bucketno, batchno;

            ninmemory++;
            ExecHashGetBucketAndBatch(hashtable, hashTuple->hashvalue,
                                    &bucketno, &batchno);

            if (batchno == curbatch)
            {
                // Keep in memory - copy to new chunks
                HashJoinTuple copyTuple = (HashJoinTuple) dense_alloc(hashtable, hashTupleSize);
                memcpy(copyTuple, hashTuple, hashTupleSize);
                copyTuple->next.unshared = hashtable->buckets.unshared[bucketno];
                hashtable->buckets.unshared[bucketno] = copyTuple;
            }
            else
            {
                // Spill to batch file
                ExecHashJoinSaveTuple(HJTUPLE_MINTUPLE(hashTuple),
                                    hashTuple->hashvalue,
                                    &hashtable->innerBatchFile[batchno],
                                    hashtable);
                hashtable->spaceUsed -= hashTupleSize;
                nfreed++;
            }

            idx += MAXALIGN(hashTupleSize);
            CHECK_FOR_INTERRUPTS();
        }

        // Free processed chunk
        pfree(oldchunks);
        oldchunks = nextchunk;
    }

    // Disable growth if rebatching was ineffective
    if (nfreed == 0 || nfreed == ninmemory)
        hashtable->growEnabled = false;
}
```