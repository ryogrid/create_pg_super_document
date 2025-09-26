# ExecParallelHashTableInsert

## Location
src/backend/executor/nodeHash.c: 1721 - 1786

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
- : HashJoinTable containing shared parallel state, bucket arrays, and batch management structures
- : TupleTableSlot containing the tuple to insert in any supported format (regular, minimal, virtual)
- : Pre-computed hash value determining the tuple's bucket and batch assignment

## Dependencies
- Functions called/Symbols referenced:
  - ExecFetchSlotMinimalTuple
  - ExecHashGetBucketAndBatch
  - BarrierPhase
  - ExecParallelHashTupleAlloc
  - HeapTupleHeaderClearMatch
  - ExecParallelHashPushTuple
  - ExecParallelHashTuplePrealloc
  - sts_puttuple
  - heap_free_minimal_tuple
- Called from (representative examples):
  - MultiExecParallelHash

## Notes and Other Information
- Uses a retry loop to handle concurrent bucket expansions and memory allocation failures
- Only operates during the PHJ_BUILD_HASH_INNER barrier phase to ensure proper synchronization
- Implements preallocation for non-current batches to reduce contention and fragmentation
- The function coordinates with parallel infrastructure including ExecParallelHashTupleAlloc for shared memory management
- Memory allocation failures trigger retries that may see updated bucket counts from concurrent expansions
- Tuple count tracking is maintained per-batch to support proper join execution across parallel workers
- Uses lock-free atomic operations for bucket list insertion via ExecParallelHashPushTuple