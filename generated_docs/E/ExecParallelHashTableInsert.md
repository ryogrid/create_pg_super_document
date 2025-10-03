# ExecParallelHashTableInsert

## Location
[src/backend/executor/nodeHash.c:1721-1786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L1721-L1786)

## Overview
Inserts a tuple into a shared hash table during parallel hash joins, coordinating memory allocation across multiple worker processes and handling both current batch insertion and future batch storage.

## Definition

```c
void
ExecParallelHashTableInsert(HashJoinTable hashtable,
							TupleTableSlot *slot,
							uint32 hashvalue)
```
## Detailed Description
This function manages tuple insertion in parallel hash joins by coordinating shared memory allocation across multiple worker processes. For batch 0 (current batch), it allocates shared memory for HashJoinTuple structures and inserts them into shared bucket arrays using lock-free atomic operations. The function implements a retry mechanism to handle memory allocation failures and bucket expansions that may occur concurrently.

For future batches (batchno > 0), the function uses a preallocation strategy where workers reserve space in shared tuple stores before inserting tuples. This prevents memory fragmentation and ensures consistent allocation patterns across parallel workers. The function integrates with the parallel hash infrastructure including barrier synchronization and shared memory management.

## Parameters / Member Variables
- `hashtable`: HashJoinTable containing shared parallel state, bucket arrays, and batch management structures
- `*slot`: TupleTableSlot containing the tuple to insert in any supported format (regular, minimal, virtual)
- `hashvalue`: Pre-computed hash value determining the tuple's bucket and batch assignment
## Dependencies
- Functions called/Symbols referenced:
  - [ExecFetchSlotMinimalTuple](ExecFetchSlotMinimalTuple.md)
  - [ExecHashGetBucketAndBatch](ExecHashGetBucketAndBatch.md)
  - [BarrierPhase](../B/BarrierPhase.md)
  - [ExecParallelHashTupleAlloc](ExecParallelHashTupleAlloc.md)
  - HeapTupleHeaderClearMatch
  - [ExecParallelHashPushTuple](ExecParallelHashPushTuple.md)
  - [ExecParallelHashTuplePrealloc](ExecParallelHashTuplePrealloc.md)
  - [sts_puttuple](../s/sts_puttuple.md)
  - [heap_free_minimal_tuple](../h/heap_free_minimal_tuple.md)
- Called from (representative examples):
  - [MultiExecParallelHash](../M/MultiExecParallelHash.md)

## Notes and Other Information
- Uses a retry loop to handle concurrent bucket expansions and memory allocation failures
- Only operates during the PHJ_BUILD_HASH_INNER barrier phase to ensure proper synchronization
- Implements preallocation for non-current batches to reduce contention and fragmentation
- The function coordinates with parallel infrastructure including ExecParallelHashTupleAlloc for shared memory management
- Memory allocation failures trigger retries that may see updated bucket counts from concurrent expansions
- Tuple count tracking is maintained per-batch to support proper join execution across parallel workers
- Uses lock-free atomic operations for bucket list insertion via ExecParallelHashPushTuple

## Simplified Source

```c
void
ExecParallelHashTableInsert(HashJoinTable hashtable,
                           TupleTableSlot *slot,
                           uint32 hashvalue)
{
    bool shouldFree;
    MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot, &shouldFree);
    dsa_pointer shared;
    int bucketno, batchno;

retry:
    // Determine which bucket and batch this tuple belongs to
    ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno, &batchno);

    if (batchno == 0)
    {
        // Current batch: insert directly into shared hash table
        HashJoinTuple hashTuple;

        // Allocate shared memory for the tuple
        hashTuple = ExecParallelHashTupleAlloc(hashtable,
                                             HJTUPLE_OVERHEAD + tuple->t_len,
                                             &shared);
        if (hashTuple == NULL)
            goto retry; // Allocation failed, retry (bucket may have expanded)

        // Store hash value and copy tuple data
        hashTuple->hashvalue = hashvalue;
        memcpy(HJTUPLE_MINTUPLE(hashTuple), tuple, tuple->t_len);
        HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

        // Insert tuple into appropriate bucket using atomic operations
        ExecParallelHashPushTuple(&hashtable->buckets.shared[bucketno],
                                hashTuple, shared);
    }
    else
    {
        // Future batch: store in shared tuple store
        size_t tuple_size = MAXALIGN(HJTUPLE_OVERHEAD + tuple->t_len);

        // Preallocate space in batch if needed
        if (hashtable->batches[batchno].preallocated < tuple_size)
        {
            if (!ExecParallelHashTuplePrealloc(hashtable, batchno, tuple_size))
                goto retry; // Preallocation failed, retry
        }

        // Use preallocated space and store tuple
        hashtable->batches[batchno].preallocated -= tuple_size;
        sts_puttuple(hashtable->batches[batchno].inner_tuples, &hashvalue, tuple);
    }

    // Update tuple count for this batch
    ++hashtable->batches[batchno].ntuples;

    // Clean up if tuple was copied
    if (shouldFree)
        heap_free_minimal_tuple(tuple);
}
```