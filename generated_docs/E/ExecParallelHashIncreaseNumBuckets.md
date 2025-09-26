# ExecParallelHashIncreaseNumBuckets

## Location
src/backend/executor/nodeHash.c: 1532 - 1630

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
  - BarrierPhase
  - BarrierArriveAndWait
  - dsa_free
  - dsa_allocate
  - dsa_get_address
  - dsa_pointer_atomic_init
  - ExecParallelHashEnsureBatchAccessors
  - ExecParallelHashTableSetCurrentBatch
  - ExecParallelHashPopChunkQueue
  - ExecHashGetBucketAndBatch
  - ExecParallelHashPushTuple
- Called from (representative examples):
  - MultiExecParallelHash
  - ExecParallelHashTupleAlloc
  - ExecParallelHashTuplePrealloc

## Notes and Other Information
- The function uses a three-phase barrier protocol to ensure consistent state across all parallel workers
- Only one worker (the elected one) performs the actual bucket array reallocation to avoid race conditions
- All workers participate in the tuple redistribution phase to maximize parallelism
- The function handles the case where new workers join during the expansion operation
- Memory management uses PostgreSQL's dynamic shared memory allocator (DSA)
- The operation is interruptible during the redistribution phase via CHECK_FOR_INTERRUPTS()