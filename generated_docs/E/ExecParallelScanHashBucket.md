# ExecParallelScanHashBucket

## Location
[src/backend/executor/nodeHash.c:2032-2082](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L2032-L2082)

## Overview
Scans a hash bucket for tuples that match the current outer tuple in a parallel hash join operation, using specialized parallel-safe tuple navigation functions.

## Definition
```c
bool ExecParallelScanHashBucket(HashJoinState *hjstate,
                                ExprContext *econtext)
```

## Detailed Description
This function is the parallel-aware version of ExecScanHashBucket, designed to safely scan hash buckets in a multi-worker parallel hash join environment. Unlike the serial version, it uses specialized parallel tuple navigation functions that handle the complexities of concurrent access to shared hash table data structures.

The key differences from the serial version:
1. Uses ExecParallelHashFirstTuple() instead of direct bucket access for starting scans
2. Uses ExecParallelHashNextTuple() instead of following hashTuple->next.unshared directly
3. Does not handle skew buckets (which are not used in parallel hash joins)
4. Provides thread-safe iteration through hash chains

The scanning logic remains fundamentally the same: iterate through the hash chain, compare hash values as a fast filter, evaluate join clauses for matches, and return the first successful match while maintaining scan state for continuation.

## Parameters / Member Variables
- `hjstate`: Hash join state containing current scan position, shared hash table, and tuple slots
- `econtext`: Expression context where the outer tuple is stored and inner tuple results are placed

## Dependencies
- Functions called/Symbols referenced:
  - [HashJoinState](../H/HashJoinState.md) (struct type)
  - [HashJoinTable](../H/HashJoinTable.md) (struct type)
  - [HashJoinTuple](../H/HashJoinTuple.md) (struct type)
  - [ExecParallelHashNextTuple](ExecParallelHashNextTuple.md) (parallel tuple navigation)
  - [ExecParallelHashFirstTuple](ExecParallelHashFirstTuple.md) (parallel bucket start)
  - [ExecStoreMinimalTuple](ExecStoreMinimalTuple.md) (tuple storage function)
  - HJTUPLE_MINTUPLE (tuple extraction macro)
  - [ExecQualAndReset](ExecQualAndReset.md) (clause evaluation function)
- Called from (representative examples):
  - [ExecHashJoinImpl](ExecHashJoinImpl.md)

## Notes and Other Information
- Designed specifically for parallel hash joins where multiple workers may access the same hash table
- Does not support skew buckets, which are a serial-only optimization
- Uses parallel-safe tuple navigation functions to avoid race conditions
- The outer tuple must be pre-loaded in econtext->ecxt_outertuple before calling
- Maintains the same interface as ExecScanHashBucket for easy substitution in parallel contexts
- Thread-safe iteration is critical for correctness in multi-worker environments

## Simplified Source

```c
bool ExecParallelScanHashBucket(HashJoinState *hjstate, ExprContext *econtext)
{
    ExprState *hjclauses = hjstate->hashclauses;
    HashJoinTable hashtable = hjstate->hj_HashTable;
    HashJoinTuple hashTuple = hjstate->hj_CurTuple;
    uint32 hashvalue = hjstate->hj_CurHashValue;

    // Determine starting point using parallel-safe functions
    if (hashTuple != NULL)
        hashTuple = ExecParallelHashNextTuple(hashtable, hashTuple); // Continue from previous
    else
        hashTuple = ExecParallelHashFirstTuple(hashtable, hjstate->hj_CurBucketNo); // Start bucket

    // Scan through hash chain looking for matches
    while (hashTuple != NULL) {
        if (hashTuple->hashvalue == hashvalue) {
            // Store tuple in execution slot for evaluation
            TupleTableSlot *inntuple = ExecStoreMinimalTuple(
                HJTUPLE_MINTUPLE(hashTuple), hjstate->hj_HashTupleSlot, false);
            econtext->ecxt_innertuple = inntuple;

            // Test join conditions
            if (ExecQualAndReset(hjclauses, econtext)) {
                hjstate->hj_CurTuple = hashTuple;
                return true; // Found match
            }
        }
        // Use parallel-safe navigation
        hashTuple = ExecParallelHashNextTuple(hashtable, hashTuple);
    }

    return false; // No match found
}
```