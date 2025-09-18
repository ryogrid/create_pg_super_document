# ExecParallelHashTableSetCurrentBatch

## Location
[src/backend/executor/nodeHash.c:3479-3499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L3479-L3499)

## Overview
Sets up a parallel hash table to work on a specific batch by configuring the table's internal state to point to the batch's bucket array and resetting chunk-related fields.

## Definition
```c
void
ExecParallelHashTableSetCurrentBatch(HashJoinTable hashtable, int batchno)
```

## Detailed Description
This function prepares a hash table for processing a specific batch in parallel hash join operations. When hash tables become too large to fit in memory, they are partitioned into multiple batches that are processed sequentially. This function switches the hash table's context to work with a particular batch by:

1. Updating the current batch number
2. Setting the bucket array pointer to point to the specified batch's buckets in shared memory
3. Copying bucket count and logarithm values from the parallel state
4. Resetting chunk management fields to prepare for tuple processing
5. Marking that this batch hasn't processed any chunks yet

The function ensures that all parallel workers can consistently access the same batch data through shared memory pointers.

## Parameters / Member Variables
- `hashtable`: The HashJoinTable structure to configure
- `batchno`: The batch number to switch to (must have valid bucket array)

## Dependencies
- Functions called/Symbols referenced:
  - [dsa_get_address](../d/dsa_get_address.md)
  - [my_log2](../m/my_log2.md)
  - InvalidDsaPointer (constant)
  - dsa_pointer_atomic (type)
  - [HashJoinTable](../H/HashJoinTable.md) (type)
  - HashMemoryChunk (type)
- Called from:
  - [MultiExecParallelHash](../M/MultiExecParallelHash.md)
  - [ExecParallelHashIncreaseNumBatches](ExecParallelHashIncreaseNumBatches.md)
  - ExecParallelHashIncreaseNumBuckets
  - [ExecParallelHashJoinNewBatch](ExecParallelHashJoinNewBatch.md)

## Notes and Other Information
- The function includes an assertion to verify the target batch has a valid bucket array
- This is a critical function for batch-based parallel hash join processing
- The function resets chunk-related fields to ensure clean state for the new batch
- Part of PostgreSQL's parallel hash join implementation for handling large datasets
- Located in src/backend/executor/nodeHash.c:3479-3499