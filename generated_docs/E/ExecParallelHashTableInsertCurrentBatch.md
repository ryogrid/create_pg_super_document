# ExecParallelHashTableInsertCurrentBatch

## Location
src/backend/executor/nodeHash.c: 1787 - 1830

## Overview
Inserts a tuple directly into the current batch of a parallel hash table without retry logic, designed for use when growth is disabled and memory allocation is guaranteed to succeed.

## Definition

```c
void
ExecParallelHashTableInsertCurrentBatch(HashJoinTable hashtable,
										TupleTableSlot *slot,
										uint32 hashvalue)
```
## Detailed Description
This is a simplified version of ExecParallelHashTableInsert specifically designed for scenarios where bucket growth has been disabled and all tuples are guaranteed to belong to the current batch. Unlike the general insertion function, this version does not handle memory allocation failures, batch switching, or retry scenarios. It assumes that ExecParallelHashTupleAlloc will always succeed and that the tuple belongs to the current batch being processed.

The function performs a straightforward insertion: allocates shared memory for the HashJoinTuple, copies the tuple data, clears the match flag, and pushes it onto the appropriate bucket list. This streamlined approach provides better performance when the preconditions are met, typically during batch reloading operations where the batch assignment and memory availability are predetermined.

## Parameters / Member Variables
- : HashJoinTable containing the parallel hash table state and current batch information
- : TupleTableSlot containing the tuple to insert, supporting all tuple formats
- : Pre-computed hash value for the tuple, used for bucket determination

## Dependencies
- Functions called/Symbols referenced:
  - ExecFetchSlotMinimalTuple
  - ExecHashGetBucketAndBatch
  - ExecParallelHashTupleAlloc
  - HeapTupleHeaderClearMatch
  - ExecParallelHashPushTuple
  - heap_free_minimal_tuple
- Called from (representative examples):
  - ExecParallelHashJoinNewBatch

## Notes and Other Information
- Assumes memory allocation will always succeed - no retry mechanism for allocation failures
- Contains an assertion to verify the tuple belongs to the current batch (batchno == hashtable->curbatch)
- Should only be used when bucket growth has been disabled to avoid allocation size changes
- Optimized for performance in scenarios where preconditions guarantee successful operation
- Typically used during batch reloading operations where tuple placement is predetermined
- Does not increment tuple counts or handle batch switching like the general insertion function
- Memory management follows the same pattern as other insertion functions with conditional tuple freeing