# ExecHashTableResetMatchFlags

## Location
[src/backend/executor/nodeHash.c:2334-2359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L2334-L2359)

## Overview
Clears all match flags in a hash table, resetting tuples to an unmatched state for reprocessing in hash join operations.

## Definition
void ExecHashTableResetMatchFlags(HashJoinTable hashtable)

## Detailed Description
This function systematically clears the HeapTupleHeaderHasMatch flags for all tuples stored in a hash table. The match flags are used during hash join operations to track which inner tuples have been successfully joined with outer tuples. Resetting these flags is necessary when rescanning or reprocessing a hash join, particularly in scenarios like:

1. Nested loop joins where the inner hash join needs to be rescanned for each outer tuple
2. Subquery execution where the same hash table is reused multiple times
3. Plan nodes that require multiple passes over the same data

The function traverses both regular hash buckets and skew buckets (used for handling hash key distribution outliers), ensuring comprehensive reset of all stored tuples.

## Parameters / Member Variables
- : The HashJoinTable containing tuples whose match flags need to be reset

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderClearMatch - clears the match flag on individual tuple headers
  - HJTUPLE_MINTUPLE - macro to extract minimal tuple from hash join tuple structure
  - [HashSkewBucket](../H/HashSkewBucket.md) - structure for handling skew bucket data
  - [HashJoinTuple](../H/HashJoinTuple.md) - tuple structure used in hash tables
- Called from (representative examples):
  - [ExecReScanHashJoin](ExecReScanHashJoin.md) - rescans hash join operations, requiring flag reset

## Notes and Other Information
- Essential for proper hash join rescanning functionality
- Handles both regular buckets and skew buckets for complete coverage
- Does not modify tuple data, only resets match status flags
- Required when the same hash table is used for multiple join operations
- Ensures correct outer join semantics when rescanning is needed
- The operation is safe and does not affect the hash table structure or tuple storage

## Simplified Source

```c
void ExecHashTableResetMatchFlags(HashJoinTable hashtable) {
    HashJoinTuple tuple;
    int i;

    // Reset match flags in all regular hash buckets
    for (i = 0; i < hashtable->nbuckets; i++) {
        for (tuple = hashtable->buckets.unshared[i]; tuple != NULL;
             tuple = tuple->next.unshared) {
            HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(tuple));
        }
    }

    // Reset match flags in skew buckets (if any)
    for (i = 0; i < hashtable->nSkewBuckets; i++) {
        int j = hashtable->skewBucketNums[i];
        HashSkewBucket *skewBucket = hashtable->skewBucket[j];

        for (tuple = skewBucket->tuples; tuple != NULL;
             tuple = tuple->next.unshared) {
            HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(tuple));
        }
    }
}
```