# ExecReScanHashJoin

## Location
[src/backend/executor/nodeHashjoin.c:1395-1482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L1395-L1482)

## Overview
Resets and rescans a hash join node, handling both hash table reuse optimization for single-batch joins and complete reconstruction for multi-batch or parameter-changed scenarios.

## Definition
```c
void ExecReScanHashJoin(HashJoinState *node)
```

## Detailed Description
This function implements the rescan logic for hash join operations, providing an important optimization for single-batch joins where the hash table can be reused. It evaluates whether the existing hash table can be preserved (single-batch join with no parameter changes), handles different join types by resetting match flags for right/anti/full joins when reusing tables, manages instrumentation and statistics collection before destroying hash tables, resets all intra-tuple state variables to their initial values, and coordinates rescanning of child plan nodes when parameters haven't changed.

The function implements a key optimization where single-batch hash tables are reused when possible, avoiding expensive rebuild operations. For multi-batch joins or when parameters change, it properly destroys the old hash table and sets up for rebuilding.

## Parameters / Member Variables
- `node`: The HashJoinState containing the hash join execution state, hash table reference, join state, and links to child plan nodes

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState
  - innerPlanState
  - HJ_FILL_INNER
  - [ExecHashTableResetMatchFlags](ExecHashTableResetMatchFlags.md)
  - castNode
  - [palloc0](../p/palloc0.md)
  - [ExecHashAccumInstrumentation](ExecHashAccumInstrumentation.md)
  - [ExecHashTableDestroy](ExecHashTableDestroy.md)
  - [ExecReScan](ExecReScan.md)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (generic executor rescan dispatcher)

## Notes and Other Information
- Implements hash table reuse optimization for single-batch joins without parameter changes
- Handles instrumentation collection before destroying hash tables to preserve performance statistics
- Resets join state to either HJ_NEED_NEW_OUTER (reusing table) or HJ_BUILD_HASHTABLE (rebuilding)
- Always resets intra-tuple state regardless of hash table reuse decision
- For multi-batch joins, currently requires complete rescan due to potential release of batch temp files
- Properly coordinates with child plan rescans, respecting parameter change indicators
- Function is declared in nodeHashjoin.h and used by the general executor rescan mechanism

## Simplified Source

```c
void ExecReScanHashJoin(HashJoinState *node) {
    PlanState *outerPlan = outerPlanState(node);
    PlanState *innerPlan = innerPlanState(node);

    // Step 1: Check if hash table can be reused (optimization)
    if (node->hj_HashTable != NULL) {
        if (node->hj_HashTable->nbatch == 1 && innerPlan->chgParam == NULL) {
            // Reuse existing hash table
            if (HJ_FILL_INNER(node)) {
                ExecHashTableResetMatchFlags(node->hj_HashTable);
            }
            node->hj_OuterNotEmpty = false;
            node->hj_JoinState = HJ_NEED_NEW_OUTER;
        } else {
            // Must rebuild hash table
            HashState *hashNode = castNode(HashState, innerPlan);

            // Collect instrumentation if needed
            if (hashNode->ps.instrument && !hashNode->hinstrument) {
                hashNode->hinstrument = palloc0(sizeof(HashInstrumentation));
            }
            if (hashNode->hinstrument) {
                ExecHashAccumInstrumentation(hashNode->hinstrument, hashNode->hashtable);
            }

            // Clean up old hash table
            hashNode->hashtable = NULL;
            ExecHashTableDestroy(node->hj_HashTable);
            node->hj_HashTable = NULL;
            node->hj_JoinState = HJ_BUILD_HASHTABLE;

            // Rescan inner plan if no parameter changes
            if (innerPlan->chgParam == NULL) {
                ExecReScan(innerPlan);
            }
        }
    }

    // Step 2: Reset intra-tuple state
    node->hj_CurHashValue = 0;
    node->hj_CurBucketNo = 0;
    node->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
    node->hj_CurTuple = NULL;
    node->hj_MatchedOuter = false;
    node->hj_FirstOuterTupleSlot = NULL;

    // Step 3: Rescan outer plan if no parameter changes
    if (outerPlan->chgParam == NULL) {
        ExecReScan(outerPlan);
    }
}
```