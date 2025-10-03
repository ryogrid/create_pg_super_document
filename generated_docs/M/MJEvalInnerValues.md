# MJEvalInnerValues

## Location
[src/backend/executor/nodeMergejoin.c:341-390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L341-L390)

## Overview
Computes the values of mergejoinable expressions for the current inner tuple and determines if matching is possible or if the join should terminate early.

## Definition

```c
static MJEvalResult
MJEvalInnerValues(MergeJoinState *mergestate, TupleTableSlot *innerslot)
```
## Detailed Description
This function is the inner tuple counterpart to MJEvalOuterValues, evaluating merge join expressions for inner tuples with similar optimization logic:

1. **Flexible tuple source**: Can evaluate expressions from either the current inner tuple or the marked inner tuple, as specified by the caller through the innerslot parameter
2. **End-of-input detection**: Returns MJEVAL_ENDOFJOIN if the provided inner tuple slot is null
3. **Expression evaluation**: Evaluates all merge clause right expressions using the inner tuple in InnerEContext
4. **NULL handling and optimization**: Detects NULL values and determines if:
   - The current tuple cannot match (MJEVAL_NONMATCHABLE)  
   - No future tuples can match, enabling early join termination (MJEVAL_ENDOFJOIN)
5. **Early termination logic**: Similar to the outer case, if a NULL appears in the first join column that sorts nulls last, and not in FillInner mode, concludes no subsequent tuples can match

The function leverages the assumption that mergejoin operators are strict to enable these performance optimizations.

## Parameters / Member Variables
- `*mergestate`: The MergeJoinState containing join state, merge clauses, and execution context
- `*innerslot`: The TupleTableSlot to evaluate (either current inner or marked inner tuple)
## Dependencies
- Functions called/Symbols referenced:
  - TupIsNull
  - ResetExprContext
  - [MemoryContextSwitchTo](MemoryContextSwitchTo.md)
  - [ExecEvalExpr](../E/ExecEvalExpr.md)
  - MJEVAL_ENDOFJOIN
  - MJEVAL_MATCHABLE
  - MJEVAL_NONMATCHABLE
- Called from:
  - [ExecMergeJoin](../E/ExecMergeJoin.md) (multiple call sites)

## Notes and Other Information
- Uses InnerEContext for expression evaluation with proper per-tuple memory management
- The flexibility to evaluate different inner tuple slots (current vs. marked) supports merge join's mark/restore mechanism
- Early termination optimization respects FillInner mode, ensuring all required inner tuples are processed when necessary
- Symmetric functionality to MJEvalOuterValues but operates on right expressions (rexpr) and inner tuple context
- Critical for merge join performance, especially in cases with many NULL values or early termination opportunities

## Simplified Source

```c
static MJEvalResult
MJEvalInnerValues(MergeJoinState *mergestate, TupleTableSlot *innerslot)
{
    ExprContext *econtext = mergestate->mj_InnerEContext;
    MJEvalResult result = MJEVAL_MATCHABLE;
    int i;
    MemoryContext oldContext;

    // Check for end of inner subplan
    if (TupIsNull(innerslot))
        return MJEVAL_ENDOFJOIN;

    // Set up evaluation context
    ResetExprContext(econtext);
    oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    econtext->ecxt_innertuple = innerslot;

    // Evaluate all merge clause expressions
    for (i = 0; i < mergestate->mj_NumClauses; i++) {
        MergeJoinClause clause = &mergestate->mj_Clauses[i];

        // Evaluate right expression for this clause
        clause->rdatum = ExecEvalExpr(clause->rexpr, econtext, &clause->risnull);

        if (clause->risnull) {
            // NULL found - check for early termination
            if (i == 0 && !clause->ssup.ssup_nulls_first && !mergestate->mj_FillInner)
                result = MJEVAL_ENDOFJOIN;  // Can end join early
            else if (result == MJEVAL_MATCHABLE)
                result = MJEVAL_NONMATCHABLE;  // This tuple can't match
        }
    }

    MemoryContextSwitchTo(oldContext);
    return result;
}
```