# ExecHashTableInsert

## Location
[src/backend/executor/nodeHash.c:1631-1720](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L1631-L1720)

## Overview
Inserts a tuple into a hash table for hash joins, either storing it in the current batch's bucket or deferring it to a temporary file for future batches based on the hash value.

## Definition

```c
void
ExecHashTableInsert(HashJoinTable hashtable,
					TupleTableSlot *slot,
					uint32 hashvalue)
```
## Detailed Description
This function handles tuple insertion for non-parallel hash joins by determining whether a tuple belongs in the current batch or should be saved for a later batch. For current batch tuples, it creates a HashJoinTuple, stores it in the appropriate bucket, and manages memory allocation. The function also implements dynamic optimization by tracking tuple density and increasing the optimal bucket count when the NTUP_PER_BUCKET threshold is exceeded. If memory usage becomes excessive, it triggers batch count increases to reduce memory pressure.

For tuples belonging to future batches, the function saves them to temporary files for later processing. The function works with various tuple formats (regular, minimal, or virtual) and handles memory management appropriately for each case.

## Parameters / Member Variables
- `hashtable`: The HashJoinTable structure containing bucket arrays, batch information, and memory tracking state
- `*slot`: TupleTableSlot containing the tuple to insert, which may be in regular, minimal, or virtual format
- `hashvalue`: Pre-computed hash value for the tuple used to determine bucket and batch placement
## Dependencies
- Functions called/Symbols referenced:
  - [ExecFetchSlotMinimalTuple](ExecFetchSlotMinimalTuple.md)
  - [ExecHashGetBucketAndBatch](ExecHashGetBucketAndBatch.md)
  - [dense_alloc](../d/dense_alloc.md)
  - HeapTupleHeaderClearMatch
  - [ExecHashIncreaseNumBatches](ExecHashIncreaseNumBatches.md)
  - [ExecHashJoinSaveTuple](ExecHashJoinSaveTuple.md)
  - [heap_free_minimal_tuple](../h/heap_free_minimal_tuple.md)
- Called from (representative examples):
  - [MultiExecPrivateHash](../M/MultiExecPrivateHash.md)
  - [ExecHashJoinNewBatch](ExecHashJoinNewBatch.md)

## Notes and Other Information
- The function always clears the tuple-matched flag on insertion, which is safe even when reloading tuples from batch files
- Dynamic bucket count optimization only occurs when there's a single batch to avoid excessive memory fragmentation
- Overflow protection prevents integer overflow and ensures allocation sizes remain within MaxAllocSize limits
- Memory tracking includes both tuple storage and projected bucket overhead to prevent memory exhaustion
- The function handles memory cleanup by freeing minimal tuples when shouldFree is true
- Tuples for future batches are stored in temporary files using the ExecHashJoinSaveTuple mechanism

## Simplified Source
```c
void ExecHashTableInsert(HashJoinTable hashtable,
                         TupleTableSlot *slot,
                         uint32 hashvalue)
{
    // Extract tuple from slot
    bool shouldFree;
    MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot, &shouldFree);
    int bucketno, batchno;

    // Determine bucket and batch for this hash value
    ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno, &batchno);

    if (batchno == hashtable->curbatch)
    {
        // Insert into current batch's hash table
        int hashTupleSize = HJTUPLE_OVERHEAD + tuple->t_len;
        HashJoinTuple hashTuple = (HashJoinTuple) dense_alloc(hashtable, hashTupleSize);

        // Store hash value and copy tuple data
        hashTuple->hashvalue = hashvalue;
        memcpy(HJTUPLE_MINTUPLE(hashTuple), tuple, tuple->t_len);
        HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

        // Insert at front of bucket chain
        hashTuple->next.unshared = hashtable->buckets.unshared[bucketno];
        hashtable->buckets.unshared[bucketno] = hashTuple;

        // Optimize bucket count if needed (single batch only)
        double ntuples = hashtable->totalTuples - hashtable->skewTuples;
        if (hashtable->nbatch == 1 &&
            ntuples > (hashtable->nbuckets_optimal * NTUP_PER_BUCKET))
        {
            // Double optimal buckets if within safe limits
            if (hashtable->nbuckets_optimal <= INT_MAX / 2 &&
                hashtable->nbuckets_optimal * 2 <= MaxAllocSize / sizeof(HashJoinTuple))
            {
                hashtable->nbuckets_optimal *= 2;
                hashtable->log2_nbuckets_optimal += 1;
            }
        }

        // Update memory tracking and handle overflow
        hashtable->spaceUsed += hashTupleSize;
        if (hashtable->spaceUsed > hashtable->spacePeak)
            hashtable->spacePeak = hashtable->spaceUsed;

        // Check if we need to increase batches to reduce memory pressure
        if (hashtable->spaceUsed +
            hashtable->nbuckets_optimal * sizeof(HashJoinTuple) > hashtable->spaceAllowed)
            ExecHashIncreaseNumBatches(hashtable);
    }
    else
    {
        // Save tuple to temp file for future batch
        ExecHashJoinSaveTuple(tuple, hashvalue,
                              &hashtable->innerBatchFile[batchno],
                              hashtable);
    }

    // Clean up temporary tuple if needed
    if (shouldFree)
        heap_free_minimal_tuple(tuple);
}
```