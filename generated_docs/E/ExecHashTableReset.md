# ExecHashTableReset

## Location
[src/backend/executor/nodeHash.c:2306-2333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L2306-L2333)

## Overview
Resets a hash table for processing a new batch in batched hash join operations, clearing all existing data and reinitializing the table structure.

## Definition
void ExecHashTableReset(HashJoinTable hashtable)

## Detailed Description
This function prepares a hash table for reuse in the next batch of a batched hash join operation. When hash joins exceed available memory, PostgreSQL uses a batching strategy where only a subset of data is processed at a time. After completing one batch, the hash table must be reset to handle the next batch.

The reset process involves:
1. Releasing all memory used by hash buckets and tuples from the previous batch
2. Resetting the batch memory context to free all allocated memory
3. Reallocating clean bucket headers for the same number of buckets
4. Resetting space usage counters and chunk tracking

This efficient approach allows reusing the same hash table structure across multiple batches without deallocating and recreating the entire table.

## Parameters / Member Variables
- : The HashJoinTable structure to reset, containing bucket arrays, memory contexts, and usage statistics

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextReset](../M/MemoryContextReset.md) - resets the batch memory context, freeing all allocated memory
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) - switches to the batch context for new allocations
  - palloc0_array - allocates and zero-initializes the new bucket array
  - [HashJoinTuple](../H/HashJoinTuple.md) - type used for bucket array allocation
- Called from (representative examples):
  - [ExecHashJoinNewBatch](ExecHashJoinNewBatch.md) - initiates processing of a new batch in batched hash joins

## Notes and Other Information
- Essential for implementing batched hash joins when working sets exceed work_mem
- Preserves the hash table structure (number of buckets) while clearing all data
- Uses memory context reset for efficient bulk deallocation
- Sets spaceUsed to 0 and chunks to NULL to reflect the clean state
- Does not modify the overall hash table configuration, only clears batch-specific data

## Simplified Source
```c
void
ExecHashTableReset(HashJoinTable hashtable)
{
    MemoryContext oldcxt;
    int nbuckets = hashtable->nbuckets;

    // Release all memory from previous batch
    MemoryContextReset(hashtable->batchCxt);
    oldcxt = MemoryContextSwitchTo(hashtable->batchCxt);

    // Reallocate clean bucket headers
    hashtable->buckets.unshared = palloc0_array(HashJoinTuple, nbuckets);
    hashtable->spaceUsed = 0;

    MemoryContextSwitchTo(oldcxt);

    // Clear chunk tracking (memory freed by context reset)
    hashtable->chunks = NULL;
}
```