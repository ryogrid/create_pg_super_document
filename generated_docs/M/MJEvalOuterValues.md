# MJEvalOuterValues

## Location
[src/backend/executor/nodeMergejoin.c:294-340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L294-L340)

## Overview
Computes the values of mergejoinable expressions for the current outer tuple and determines if matching is possible or if the join should terminate early.

## Definition

```c
static MJEvalResult
MJEvalOuterValues(MergeJoinState *mergestate)
```
## Detailed Description
This function evaluates the merge join expressions for the current outer tuple and performs several critical optimizations:

1. **End-of-input detection**: Returns MJEVAL_ENDOFJOIN if the outer tuple slot is null
2. **Expression evaluation**: Evaluates all merge clause expressions using the outer tuple in OuterEContext
3. **NULL handling and optimization**: Detects NULL values in merge expressions and determines if:
   - The current tuple cannot match (MJEVAL_NONMATCHABLE)
   - No future tuples can match, allowing early join termination (MJEVAL_ENDOFJOIN)
4. **Early termination logic**: If a NULL appears in the first join column that sorts nulls last, and not in FillOuter mode, concludes that no subsequent tuples can match since they must all have NULLs in the first column

The function assumes that mergejoin operators are strict (return NULL when any input is NULL), which enables these optimizations.

## Parameters / Member Variables
- : The MergeJoinState containing the current join state, tuple slots, merge clauses, and execution context

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
- Uses OuterEContext for expression evaluation, which can be reset for each new tuple
- Memory context switching ensures proper cleanup of per-tuple allocations
- The early termination optimization is only applied when not in FillOuter mode, since FillOuter requires visiting all outer tuples
- Returns MJEvalResult enum values to indicate the evaluation outcome to the caller
- Critical for merge join performance as it can eliminate unnecessary tuple processing