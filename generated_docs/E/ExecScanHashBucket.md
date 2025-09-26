# ExecScanHashBucket

## Location
[src/backend/executor/nodeHash.c:1971-2031](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L1971-L2031)

## Overview
Scans a hash bucket for tuples that match the current outer tuple in a hash join operation, handling both regular buckets and skew buckets.

## Definition
```c
bool ExecScanHashBucket(HashJoinState *hjstate,
                        ExprContext *econtext)
```

## Detailed Description
This function performs the core lookup operation in hash joins by scanning through a hash bucket to find inner tuples that match the current outer tuple. The function handles the continuation of scanning from a previous position and supports both regular hash buckets and specialized skew buckets for handling hash value distribution problems.

The scanning process:
1. Determines the starting point for the scan (either continuing from hj_CurTuple or starting fresh)
2. Chooses between skew bucket or regular bucket based on hj_CurSkewBucketNo
3. Iterates through the hash chain, comparing hash values for efficiency
4. For matching hash values, stores the tuple in the execution slot and evaluates join clauses
5. Returns the first matching tuple found, or false if no matches exist

The function maintains scan state in hjstate->hj_CurTuple to support resuming the scan on subsequent calls, which is essential for handling multiple matches within the same bucket.

## Parameters / Member Variables
- `hjstate`: Hash join state containing current scan position, hash table, and tuple slots
- `econtext`: Expression context where the outer tuple is stored and inner tuple results are placed

## Dependencies
- Functions called/Symbols referenced:
  - [HashJoinState](../H/HashJoinState.md) (struct type)
  - [HashJoinTable](../H/HashJoinTable.md) (struct type) 
  - [HashJoinTuple](../H/HashJoinTuple.md) (struct type)
  - INVALID_SKEW_BUCKET_NO (constant)
  - [ExecStoreMinimalTuple](ExecStoreMinimalTuple.md) (tuple storage function)
  - HJTUPLE_MINTUPLE (tuple extraction macro)
  - [ExecQualAndReset](ExecQualAndReset.md) (clause evaluation function)
- Called from (representative examples):
  - [ExecHashJoinImpl](ExecHashJoinImpl.md)

## Notes and Other Information
- The outer tuple must be pre-loaded in econtext->ecxt_outertuple before calling
- Uses hash value comparison as a fast pre-filter before expensive join clause evaluation
- Supports skew buckets for handling poorly distributed hash values
- Maintains scan state to allow multiple calls for finding all matches in a bucket
- Returns immediately upon finding the first match, requiring multiple calls to find all matches
- The function does not pfree the stored tuple, leaving memory management to the caller