# ExecParallelHashCloseBatchAccessors

## Location
src/backend/executor/nodeHash.c: 3184 - 3204

## Overview
Properly closes and cleans up all ParallelHashJoinBatchAccessor objects, ensuring shared tuplestores are properly terminated and memory is freed.

## Definition


## Detailed Description
 performs cleanup operations for parallel hash join batch accessors. This function ensures that all shared tuplestore operations are properly terminated before freeing the accessor array memory.

The function performs two main cleanup operations for each batch:
1. **Tuplestore termination**: Calls  and  on both inner and outer tuplestores to properly close any open file handles and finalize tuplestore state
2. **Memory cleanup**: Frees the batch accessor array and sets the pointer to NULL to prevent accidental reuse

This cleanup is essential for proper resource management in parallel hash joins, particularly when the number of batches needs to be increased due to memory pressure or when the hash join operation completes. The function ensures that no file descriptors are leaked and that shared tuplestore resources are properly released.

## Parameters / Member Variables
- : HashJoinTable containing the batch accessor array to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - sts_end_write (terminate shared tuplestore write operations)
  - sts_end_parallel_scan (terminate shared tuplestore parallel scan operations)
  - pfree (free allocated memory)
- Called from:
  - ExecParallelHashIncreaseNumBatches (nodeHash.c:1116, 1214) 
  - ExecParallelHashEnsureBatchAccessors (nodeHash.c:3216)

## Notes and Other Information
- This is a static function internal to nodeHash.c for parallel hash join cleanup
- Must be called before reallocating batch accessors or ending hash join operations
- Ensures proper cleanup of both inner and outer tuplestore resources for all batches  
- Sets hashtable->batches to NULL after cleanup to prevent accidental reuse
- Critical for preventing file descriptor leaks in parallel hash join operations
- The function handles cleanup for the local backend's accessor array, not the shared batch structures
- Should be paired with ExecParallelHashJoinSetUpBatches() or ExecParallelHashEnsureBatchAccessors() calls