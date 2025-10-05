# ExecHashJoinImpl

## Location
[src/backend/executor/nodeHashjoin.c:220-677](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L220-L677)

## Overview
ExecHashJoinImpl implements the core Hybrid Hash Join algorithm for PostgreSQL, handling both parallel and non-parallel execution through an inline-optimized state machine that processes hash joins for all join types.

## Definition

```c
static pg_attribute_always_inline TupleTableSlot *
ExecHashJoinImpl(PlanState *pstate, bool parallel)
```
## Detailed Description
ExecHashJoinImpl is the heart of PostgreSQL's hash join execution engine, implementing the Hybrid Hash Join algorithm through a comprehensive state machine. The function is marked with  to allow compilers to create specialized versions for parallel and non-parallel execution, optimizing away unnecessary branches.

The algorithm operates by building a hash table on the "inner" relation and probing it with tuples from the "outer" relation. It handles complex scenarios including:

- Multi-batch processing when memory is insufficient for the entire hash table
- Empty outer relation optimization to avoid unnecessary hash table construction
- Various join types (inner, left, right, anti, semi, full)
- Parallel execution coordination through barriers
- Skew bucket handling for performance optimization
- Unmatched tuple processing for outer joins

The state machine progresses through several key states:
1. **HJ_BUILD_HASHTABLE**: Initial hash table construction
2. **HJ_NEED_NEW_OUTER**: Fetching outer tuples for probing
3. **HJ_SCAN_BUCKET**: Scanning hash buckets for matches
4. **HJ_FILL_OUTER_TUPLE**: Handling unmatched outer tuples in outer joins
5. **HJ_FILL_INNER_TUPLES**: Processing unmatched inner tuples
6. **HJ_NEED_NEW_BATCH**: Advancing to next batch in multi-batch scenarios

## Parameters / Member Variables
- `*pstate`: The PlanState node containing hash join execution state
- `parallel`: Boolean flag indicating whether to use parallel hash join execution paths
## Dependencies
- Functions called/Symbols referenced:
  - [ExecHashTableCreate](ExecHashTableCreate.md): Creates the hash table structure
  - [ExecHashJoinOuterGetTuple](ExecHashJoinOuterGetTuple.md)/ExecParallelHashJoinOuterGetTuple: Retrieves outer tuples
  - [ExecScanHashBucket](ExecScanHashBucket.md)/ExecParallelScanHashBucket: Scans hash buckets for matches
  - [ExecHashJoinNewBatch](ExecHashJoinNewBatch.md)/ExecParallelHashJoinNewBatch: Advances to next batch
  - [ExecQual](ExecQual.md): Evaluates join and filter conditions
  - [ExecProject](ExecProject.md): Projects result tuples
  - [BarrierArriveAndWait](../B/BarrierArriveAndWait.md): Synchronizes parallel workers

- Called from:
  - ExecHashJoin: Non-parallel hash join entry point
  - ExecParallelHashJoin: Parallel hash join entry point

## Notes and Other Information
The function implements several important optimizations:
- **Empty outer optimization**: Checks if outer relation is empty before building hash table for certain join types
- **Batch processing**: Handles cases where hash table doesn't fit in memory by processing data in batches
- **Parallel coordination**: Uses barriers to coordinate parallel workers during hash table construction and batch processing
- **Skew handling**: Special processing for highly skewed data using dedicated skew buckets

The always-inline attribute is crucial for performance, allowing the compiler to eliminate the parallel branch checks in specialized contexts, reducing runtime overhead.

Location: src/backend/executor/nodeHashjoin.c:220-677

## Simplified Source

```c
static pg_attribute_always_inline TupleTableSlot *
ExecHashJoinImpl(PlanState *pstate, bool parallel)
{
    HashJoinState *node = castNode(HashJoinState, pstate);
    PlanState *outerNode = outerPlanState(node);
    HashState *hashNode = (HashState *) innerPlanState(node);
    ExprState *joinqual = node->js.joinqual;
    ExprState *otherqual = node->js.ps.qual;
    ExprContext *econtext = node->js.ps.ps_ExprContext;
    HashJoinTable hashtable = node->hj_HashTable;
    TupleTableSlot *outerTupleSlot;
    uint32 hashvalue;
    int batchno;

    // Reset per-tuple memory context
    ResetExprContext(econtext);

    // Hash join state machine
    for (;;) {
        CHECK_FOR_INTERRUPTS();

        switch (node->hj_JoinState) {
            case HJ_BUILD_HASHTABLE:
                // Build hash table for inner relation
                Assert(hashtable == NULL);

                // Check for empty outer relation optimization
                if (HJ_FILL_INNER(node)) {
                    node->hj_FirstOuterTupleSlot = NULL;
                } else if (parallel) {
                    // Skip empty-outer optimization for parallel joins
                    node->hj_FirstOuterTupleSlot = NULL;
                } else if (HJ_FILL_OUTER(node) ||
                          (outerNode->plan->startup_cost < hashNode->ps.plan->total_cost &&
                           !node->hj_OuterNotEmpty)) {
                    // Try to fetch first outer tuple
                    node->hj_FirstOuterTupleSlot = ExecProcNode(outerNode);
                    if (TupIsNull(node->hj_FirstOuterTupleSlot)) {
                        return NULL;  // Empty outer relation
                    }
                    node->hj_OuterNotEmpty = true;
                }

                // Create hash table
                hashtable = ExecHashTableCreate(hashNode, node->hj_HashOperators,
                                              node->hj_Collations, HJ_FILL_INNER(node));
                node->hj_HashTable = hashtable;

                // Build the hash table
                hashNode->hashtable = hashtable;
                (void) MultiExecProcNode((PlanState *) hashNode);

                // Check for empty inner relation
                if (hashtable->totalTuples == 0 && !HJ_FILL_OUTER(node)) {
                    return NULL;
                }

                hashtable->nbatch_outstart = hashtable->nbatch;
                node->hj_OuterNotEmpty = false;

                if (parallel) {
                    // Handle parallel batch setup
                    node->hj_JoinState = HJ_NEED_NEW_BATCH;
                    continue;
                } else {
                    node->hj_JoinState = HJ_NEED_NEW_OUTER;
                }
                // FALL THRU

            case HJ_NEED_NEW_OUTER:
                // Get next outer tuple
                if (parallel) {
                    outerTupleSlot = ExecParallelHashJoinOuterGetTuple(outerNode, node, &hashvalue);
                } else {
                    outerTupleSlot = ExecHashJoinOuterGetTuple(outerNode, node, &hashvalue);
                }

                if (TupIsNull(outerTupleSlot)) {
                    // End of batch - handle unmatched inner tuples if needed
                    if (HJ_FILL_INNER(node)) {
                        if (parallel) {
                            if (ExecParallelPrepHashTableForUnmatched(node))
                                node->hj_JoinState = HJ_FILL_INNER_TUPLES;
                            else
                                node->hj_JoinState = HJ_NEED_NEW_BATCH;
                        } else {
                            ExecPrepHashTableForUnmatched(node);
                            node->hj_JoinState = HJ_FILL_INNER_TUPLES;
                        }
                    } else {
                        node->hj_JoinState = HJ_NEED_NEW_BATCH;
                    }
                    continue;
                }

                econtext->ecxt_outertuple = outerTupleSlot;
                node->hj_MatchedOuter = false;

                // Find hash bucket for this tuple
                node->hj_CurHashValue = hashvalue;
                ExecHashGetBucketAndBatch(hashtable, hashvalue, &node->hj_CurBucketNo, &batchno);
                node->hj_CurSkewBucketNo = ExecHashGetSkewBucket(hashtable, hashvalue);
                node->hj_CurTuple = NULL;

                // Check if tuple belongs to current batch
                if (batchno != hashtable->curbatch &&
                    node->hj_CurSkewBucketNo == INVALID_SKEW_BUCKET_NO) {
                    // Save tuple for later batch
                    bool shouldFree;
                    MinimalTuple mintuple = ExecFetchSlotMinimalTuple(outerTupleSlot, &shouldFree);
                    ExecHashJoinSaveTuple(mintuple, hashvalue,
                                        &hashtable->outerBatchFile[batchno], hashtable);
                    if (shouldFree)
                        heap_free_minimal_tuple(mintuple);
                    continue;
                }

                node->hj_JoinState = HJ_SCAN_BUCKET;
                // FALL THRU

            case HJ_SCAN_BUCKET:
                // Scan hash bucket for matches
                if (parallel) {
                    if (!ExecParallelScanHashBucket(node, econtext)) {
                        node->hj_JoinState = HJ_FILL_OUTER_TUPLE;
                        continue;
                    }
                } else {
                    if (!ExecScanHashBucket(node, econtext)) {
                        node->hj_JoinState = HJ_FILL_OUTER_TUPLE;
                        continue;
                    }
                }

                // Test join and other qualifications
                if (joinqual == NULL || ExecQual(joinqual, econtext)) {
                    node->hj_MatchedOuter = true;
                    HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTuple));

                    // Handle different join types
                    if (node->js.jointype == JOIN_ANTI) {
                        node->hj_JoinState = HJ_NEED_NEW_OUTER;
                        continue;
                    }
                    if (node->js.single_match)
                        node->hj_JoinState = HJ_NEED_NEW_OUTER;
                    if (node->js.jointype == JOIN_RIGHT_ANTI)
                        continue;

                    if (otherqual == NULL || ExecQual(otherqual, econtext))
                        return ExecProject(node->js.ps.ps_ProjInfo);
                }
                break;

            case HJ_FILL_OUTER_TUPLE:
                // Handle unmatched outer tuples for outer joins
                node->hj_JoinState = HJ_NEED_NEW_OUTER;
                if (!node->hj_MatchedOuter && HJ_FILL_OUTER(node)) {
                    econtext->ecxt_innertuple = node->hj_NullInnerTupleSlot;
                    if (otherqual == NULL || ExecQual(otherqual, econtext))
                        return ExecProject(node->js.ps.ps_ProjInfo);
                }
                break;

            case HJ_FILL_INNER_TUPLES:
                // Emit unmatched inner tuples for right/full joins
                if (!(parallel ? ExecParallelScanHashTableForUnmatched(node, econtext)
                              : ExecScanHashTableForUnmatched(node, econtext))) {
                    node->hj_JoinState = HJ_NEED_NEW_BATCH;
                    continue;
                }
                econtext->ecxt_outertuple = node->hj_NullOuterTupleSlot;
                if (otherqual == NULL || ExecQual(otherqual, econtext))
                    return ExecProject(node->js.ps.ps_ProjInfo);
                break;

            case HJ_NEED_NEW_BATCH:
                // Advance to next batch
                if (parallel) {
                    if (!ExecParallelHashJoinNewBatch(node))
                        return NULL;
                } else {
                    if (!ExecHashJoinNewBatch(node))
                        return NULL;
                }
                node->hj_JoinState = HJ_NEED_NEW_OUTER;
                break;

            default:
                elog(ERROR, "unrecognized hashjoin state: %d", (int) node->hj_JoinState);
        }
    }
}
```