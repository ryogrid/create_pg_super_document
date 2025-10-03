# MultiExecPrivateHash

## Location
[src/backend/executor/nodeHash.c:138-213](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L138-L213)

## Overview
MultiExecPrivateHash implements the single-backend hash table building algorithm, consuming all input tuples and inserting them into a backend-private hash table with support for batching and skew optimization.

## Definition

```c
static void
MultiExecPrivateHash(HashState *node)
```
## Detailed Description
MultiExecPrivateHash is the parallel-oblivious version of hash table building, designed for single-backend execution. It builds a complete hash table by consuming all tuples from the child plan node and inserting them using hash-based placement with several optimization strategies.

The function implements a sophisticated tuple processing pipeline: it computes hash values for each input tuple, determines optimal bucket placement (including skew bucket optimization for frequently occurring values), and handles dynamic resizing when bucket capacity is exceeded. The algorithm also manages memory usage by tracking space consumption and potentially creating batch files when memory constraints are encountered.

Key optimizations include skew bucket handling for values that occur much more frequently than average, and dynamic bucket resizing when the load factor (NTUP_PER_BUCKET) is exceeded to maintain optimal hash table performance.

## Parameters / Member Variables
- `*node`: HashState containing execution state, hash table reference, and expression context
## Dependencies
- Functions called/Symbols referenced:
  - [HashState](../H/HashState.md) (parameter and internal references)
  - [HashJoinTable](../H/HashJoinTable.md) (hash table structure)
  - outerPlanState (access to child plan node)
  - [ExecProcNode](../E/ExecProcNode.md) (execute child node)
  - TupIsNull (check for end of input)
  - [ExecHashGetHashValue](../E/ExecHashGetHashValue.md) (compute hash values)
  - [ExecHashGetSkewBucket](../E/ExecHashGetSkewBucket.md) (check for skew optimization)
  - INVALID_SKEW_BUCKET_NO (skew bucket validation)
  - [ExecHashSkewTableInsert](../E/ExecHashSkewTableInsert.md) (insert into skew buckets)
  - [ExecHashTableInsert](../E/ExecHashTableInsert.md) (standard hash table insertion)
  - [ExecHashIncreaseNumBuckets](../E/ExecHashIncreaseNumBuckets.md) (dynamic resizing)
  - [HashJoinTuple](../H/HashJoinTuple.md) (tuple structure for space calculations)
- Called from (representative examples):
  - [MultiExecHash](MultiExecHash.md) (single-backend execution path)

## Notes and Other Information
- Handles skew optimization by detecting frequently occurring hash values and placing them in specialized buckets
- Implements dynamic hash table resizing when optimal bucket count is not achieved initially
- Tracks memory usage including space for buckets in spaceUsed metrics for EXPLAIN ANALYZE
- Updates both totalTuples (all processed tuples) and partialTuples (tuples in current partition) counters
- May create temporary batch files when memory constraints require partitioning large datasets
- Located in src/backend/executor/nodeHash.c:138-213

## Simplified Source

```c
static void MultiExecPrivateHash(HashState *node)
{
    PlanState *outerNode;
    List *hashkeys;
    HashJoinTable hashtable;
    TupleTableSlot *slot;
    ExprContext *econtext;
    uint32 hashvalue;

    // Initialize state from node
    outerNode = outerPlanState(node);
    hashtable = node->hashtable;
    hashkeys = node->hashkeys;
    econtext = node->ps.ps_ExprContext;

    // Process all input tuples
    for (;;) {
        slot = ExecProcNode(outerNode);
        if (TupIsNull(slot))
            break;

        // Compute hash value for tuple
        econtext->ecxt_outertuple = slot;
        if (ExecHashGetHashValue(hashtable, econtext, hashkeys, false, hashtable->keepNulls, &hashvalue)) {
            int bucketNumber;

            // Check for skew bucket optimization
            bucketNumber = ExecHashGetSkewBucket(hashtable, hashvalue);
            if (bucketNumber != INVALID_SKEW_BUCKET_NO) {
                // Insert into skew bucket
                ExecHashSkewTableInsert(hashtable, slot, hashvalue, bucketNumber);
                hashtable->skewTuples += 1;
            } else {
                // Insert normally
                ExecHashTableInsert(hashtable, slot, hashvalue);
            }
            hashtable->totalTuples += 1;
        }
    }

    // Resize hash table if needed
    if (hashtable->nbuckets != hashtable->nbuckets_optimal)
        ExecHashIncreaseNumBuckets(hashtable);

    // Account for bucket space usage
    hashtable->spaceUsed += hashtable->nbuckets * sizeof(HashJoinTuple);
    if (hashtable->spaceUsed > hashtable->spacePeak)
        hashtable->spacePeak = hashtable->spaceUsed;

    hashtable->partialTuples = hashtable->totalTuples;
}
```