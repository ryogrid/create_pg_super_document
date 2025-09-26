# ExecHashRemoveNextSkewBucket

## Location
src/backend/executor/nodeHash.c: 2627 - 2740

## Overview
Removes the least valuable skew bucket by relocating its tuples to the main hash table or batch files, and manages the cleanup of skew optimization structures.

## Definition


## Detailed Description
ExecHashRemoveNextSkewBucket implements the memory management strategy for skew optimization by removing the least important skew bucket when memory pressure occurs. The function selects the bucket to remove based on the reverse order of creation (LIFO), ensuring that the least common MCVs are removed first since skew buckets are created in decreasing order of MCV frequency.

The function processes all tuples in the selected skew bucket and redistributes them based on their target batch. Tuples belonging to the current batch are moved to the main hash table using dense storage allocation, while tuples for future batches are written to temporary files. This ensures no data is lost during the skew bucket removal process.

A critical aspect of this function is maintaining the integrity of the skew hash table's open addressing scheme. Since multiple hash values can collide and be stored in different bucket positions, removing buckets in any order other than reverse creation order could break lookup functionality. The function handles this by always removing the most recently created bucket.

When all skew buckets are removed, the function disables skew optimization entirely and frees all associated data structures.

## Parameters / Member Variables
- : The HashJoinTable containing the skew buckets to be managed and cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - ExecHashGetBucketAndBatch (determines target bucket and batch for tuples)
  - dense_alloc (allocates dense storage for tuples moved to main hash table)
  - ExecHashJoinSaveTuple (saves tuples to batch files for future processing)
  - pfree (frees memory for hash tuples and bucket structures)
  - CHECK_FOR_INTERRUPTS (allows cancellation during tuple processing)
- Called from:
  - ExecHashSkewTableInsert (when skew space limit is exceeded)

## Notes and Other Information
- Always removes buckets in reverse order of creation to maintain hash table lookup integrity
- Tuples are redistributed rather than lost: current batch tuples go to main hash table, future batch tuples go to temp files
- Uses dense storage allocation for main hash table tuples to ensure they're found by other operations like ExecHashIncreaseNumBatches
- Maintains separate accounting for total space usage (spaceUsed) and skew-specific usage (spaceUsedSkew)
- Automatically disables skew optimization when no buckets remain
- Includes cancellation points during tuple processing loop for responsiveness
- Critical that removal order matches reverse creation order to prevent open addressing hash table corruption
- Frees both tuple data and bucket overhead when cleaning up structures