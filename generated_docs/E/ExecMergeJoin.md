# ExecMergeJoin

## Location
[src/backend/executor/nodeMergejoin.c:599-1443](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L599-L1443)

## Overview
The core execution function that implements the merge join algorithm using a sophisticated state machine to efficiently join two pre-sorted input streams.

## Definition
```c
static TupleTableSlot *ExecMergeJoin(PlanState *pstate)
```

## Detailed Description
ExecMergeJoin implements PostgreSQL's merge join algorithm, one of the three fundamental join algorithms (along with nested loop and hash join). This function operates as a complex state machine that efficiently joins two pre-sorted input streams by advancing through both streams in a coordinated manner, taking advantage of the sort order to avoid redundant comparisons.

The algorithm maintains multiple execution states to handle different phases of the join process, including initialization, tuple comparison, skipping non-matching tuples, and handling outer join semantics. The state machine approach allows the function to be called repeatedly, returning one result tuple per call while maintaining its position in both input streams.

Key algorithmic features include:
- **State-driven execution**: Uses a comprehensive state machine with states like EXEC_MJ_INITIALIZE_OUTER, EXEC_MJ_SKIP_TEST, EXEC_MJ_JOINTUPLES
- **Mark and restore capability**: Supports backing up in the inner stream when duplicate values are found in the outer stream
- **Outer join support**: Handles LEFT, RIGHT, FULL, ANTI, and RIGHT_ANTI join types with proper null-filling logic
- **Memory efficiency**: Processes tuples one at a time without materializing entire result sets
- **Sort order dependency**: Requires both input streams to be sorted on the join keys

The function handles complex scenarios including duplicate values across join boundaries, proper outer join semantics, and various optimization flags like single_match for semi-joins.

## Parameters / Member Variables
- `pstate`: Pointer to the PlanState structure, which is cast to MergeJoinState to access merge join specific state and configuration

## Dependencies
- Functions called/Symbols referenced:
  - castNode (for type casting)
  - innerPlanState/outerPlanState (to access child plan nodes)
  - ResetExprContext (memory management)
  - [ExecProcNode](ExecProcNode.md) (to fetch tuples from child nodes)
  - [MJEvalOuterValues](../M/MJEvalOuterValues.md)/MJEvalInnerValues (join key evaluation)
  - [MJCompare](../M/MJCompare.md) (tuple comparison)
  - [MJFillOuter](../M/MJFillOuter.md)/MJFillInner (outer join null-filling)
  - [ExecQual](ExecQual.md) (qualification testing)
  - [ExecProject](ExecProject.md) (result tuple projection)
  - [ExecMarkPos](ExecMarkPos.md)/ExecRestrPos (mark and restore operations)
  - MarkInnerTuple (marking functionality)
- Called from (representative examples):
  - [ExecInitMergeJoin](ExecInitMergeJoin.md) (sets as the execution function)

## Notes and Other Information
- Implements a complex state machine with 11 distinct execution states
- Requires input streams to be sorted on join keys; violation results in runtime error
- Supports all PostgreSQL join types including outer joins and anti-joins
- Uses mark/restore capability of inner plan when available for handling duplicate join keys
- Memory context is reset per tuple to prevent memory leaks during long-running joins
- Includes extensive debugging support through MJ_printf and MJ_dump macros
- The function can be interrupted via CHECK_FOR_INTERRUPTS() for query cancellation
- Performance depends heavily on the sort order and distribution of join keys
- State transitions are carefully designed to handle edge cases like end-of-stream conditions

## Simplified Source

```c
static TupleTableSlot *
ExecMergeJoin(PlanState *pstate)
{
    MergeJoinState *node = castNode(MergeJoinState, pstate);
    ExprState *joinqual = node->js.joinqual;
    ExprState *otherqual = node->js.ps.qual;
    PlanState *innerPlan = innerPlanState(node);
    PlanState *outerPlan = outerPlanState(node);
    ExprContext *econtext = node->js.ps.ps_ExprContext;
    bool doFillOuter = node->mj_FillOuter;
    bool doFillInner = node->mj_FillInner;

    CHECK_FOR_INTERRUPTS();
    ResetExprContext(econtext);

    for (;;) {
        switch (node->mj_JoinState) {
            case EXEC_MJ_INITIALIZE_OUTER:
                // Get first outer tuple and evaluate join values
                outerTupleSlot = ExecProcNode(outerPlan);
                node->mj_OuterTupleSlot = outerTupleSlot;

                switch (MJEvalOuterValues(node)) {
                    case MJEVAL_MATCHABLE:
                        node->mj_JoinState = EXEC_MJ_INITIALIZE_INNER;
                        break;
                    case MJEVAL_NONMATCHABLE:
                        if (doFillOuter) {
                            TupleTableSlot *result = MJFillOuter(node);
                            if (result) return result;
                        }
                        break;
                    case MJEVAL_ENDOFJOIN:
                        if (doFillInner) {
                            node->mj_JoinState = EXEC_MJ_ENDOUTER;
                            node->mj_MatchedInner = true;
                            break;
                        }
                        return NULL;
                }
                break;

            case EXEC_MJ_INITIALIZE_INNER:
                // Get first inner tuple and evaluate join values
                innerTupleSlot = ExecProcNode(innerPlan);
                node->mj_InnerTupleSlot = innerTupleSlot;

                switch (MJEvalInnerValues(node, innerTupleSlot)) {
                    case MJEVAL_MATCHABLE:
                        node->mj_JoinState = EXEC_MJ_SKIP_TEST;
                        break;
                    case MJEVAL_NONMATCHABLE:
                        if (doFillInner) {
                            TupleTableSlot *result = MJFillInner(node);
                            if (result) return result;
                        }
                        break;
                    case MJEVAL_ENDOFJOIN:
                        if (doFillOuter) {
                            node->mj_JoinState = EXEC_MJ_ENDINNER;
                            node->mj_MatchedOuter = false;
                            break;
                        }
                        return NULL;
                }
                break;

            case EXEC_MJ_JOINTUPLES:
                // Join matching tuples
                node->mj_JoinState = EXEC_MJ_NEXTINNER;

                outerTupleSlot = node->mj_OuterTupleSlot;
                innerTupleSlot = node->mj_InnerTupleSlot;
                econtext->ecxt_outertuple = outerTupleSlot;
                econtext->ecxt_innertuple = innerTupleSlot;

                bool qualResult = (joinqual == NULL || ExecQual(joinqual, econtext));

                if (qualResult) {
                    node->mj_MatchedOuter = true;
                    node->mj_MatchedInner = true;

                    // Handle special join types
                    if (node->js.jointype == JOIN_ANTI) {
                        node->mj_JoinState = EXEC_MJ_NEXTOUTER;
                        break;
                    }

                    if (node->js.single_match)
                        node->mj_JoinState = EXEC_MJ_NEXTOUTER;

                    if (node->js.jointype == JOIN_RIGHT_ANTI)
                        break;

                    qualResult = (otherqual == NULL || ExecQual(otherqual, econtext));

                    if (qualResult)
                        return ExecProject(node->js.ps.ps_ProjInfo);
                }
                break;

            case EXEC_MJ_SKIP_TEST:
                // Compare tuples and skip non-matching ones
                int compareResult = MJCompare(node);

                if (compareResult == 0) {
                    if (!node->mj_SkipMarkRestore)
                        ExecMarkPos(innerPlan);
                    MarkInnerTuple(node->mj_InnerTupleSlot, node);
                    node->mj_JoinState = EXEC_MJ_JOINTUPLES;
                } else if (compareResult < 0) {
                    node->mj_JoinState = EXEC_MJ_SKIPOUTER_ADVANCE;
                } else {
                    node->mj_JoinState = EXEC_MJ_SKIPINNER_ADVANCE;
                }
                break;

            case EXEC_MJ_SKIPOUTER_ADVANCE:
                // Skip to next outer tuple
                if (doFillOuter && !node->mj_MatchedOuter) {
                    node->mj_MatchedOuter = true;
                    TupleTableSlot *result = MJFillOuter(node);
                    if (result) return result;
                }

                outerTupleSlot = ExecProcNode(outerPlan);
                node->mj_OuterTupleSlot = outerTupleSlot;
                node->mj_MatchedOuter = false;

                switch (MJEvalOuterValues(node)) {
                    case MJEVAL_MATCHABLE:
                        node->mj_JoinState = EXEC_MJ_SKIP_TEST;
                        break;
                    case MJEVAL_ENDOFJOIN:
                        if (doFillInner && !TupIsNull(node->mj_InnerTupleSlot)) {
                            node->mj_JoinState = EXEC_MJ_ENDOUTER;
                            break;
                        }
                        return NULL;
                }
                break;

            case EXEC_MJ_SKIPINNER_ADVANCE:
                // Skip to next inner tuple
                if (doFillInner && !node->mj_MatchedInner) {
                    node->mj_MatchedInner = true;
                    TupleTableSlot *result = MJFillInner(node);
                    if (result) return result;
                }

                innerTupleSlot = ExecProcNode(innerPlan);
                node->mj_InnerTupleSlot = innerTupleSlot;
                node->mj_MatchedInner = false;

                switch (MJEvalInnerValues(node, innerTupleSlot)) {
                    case MJEVAL_MATCHABLE:
                        node->mj_JoinState = EXEC_MJ_SKIP_TEST;
                        break;
                    case MJEVAL_ENDOFJOIN:
                        if (doFillOuter && !TupIsNull(node->mj_OuterTupleSlot)) {
                            node->mj_JoinState = EXEC_MJ_ENDINNER;
                            break;
                        }
                        return NULL;
                }
                break;

            // Additional states (ENDOUTER, ENDINNER, etc.) handle outer join null-filling
            // when one input stream is exhausted but the other still has unmatched tuples

            default:
                elog(ERROR, "unrecognized mergejoin state: %d", (int) node->mj_JoinState);
        }
    }
}
```