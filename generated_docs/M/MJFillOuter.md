# MJFillOuter

## Location
[src/backend/executor/nodeMergejoin.c:452-482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L452-L482)

## Overview
Generates a fake join tuple with NULLs for the inner tuple columns to handle unmatched outer tuples in left outer joins.

## Definition

```c
static TupleTableSlot *
MJFillOuter(MergeJoinState *node)
```
## Detailed Description
This function implements the outer join logic for merge joins by creating result tuples when an outer tuple has no matching inner tuples. The function:

1. **Context setup**: Sets up the expression context with the current outer tuple and a pre-allocated NULL inner tuple slot
2. **Qualification testing**: Applies any non-join qualifiers (WHERE clause conditions) to the combination of outer tuple + NULL inner tuple
3. **Projection**: If qualifications pass, projects the result tuple using the join's projection information
4. **Instrumentation**: Updates query execution statistics for filtered tuples when qualifications fail

This function is essential for implementing LEFT OUTER JOIN and FULL OUTER JOIN semantics, ensuring that unmatched outer tuples are included in the result set with NULL values for inner columns.

## Parameters / Member Variables
- : The MergeJoinState containing the join execution state, outer tuple slot, NULL inner tuple slot, and projection information

## Dependencies
- Functions called/Symbols referenced:
  - ResetExprContext
  - [ExecQual](../E/ExecQual.md)
  - [ExecProject](../E/ExecProject.md)
  - InstrCountFiltered2
  - MJ_printf (debug macro)
- Called from:
  - [ExecMergeJoin](../E/ExecMergeJoin.md) (multiple call sites)

## Notes and Other Information
- Returns a TupleTableSlot containing the projected result tuple, or NULL if non-join qualifications fail
- Uses a pre-allocated NULL inner tuple slot (mj_NullInnerTupleSlot) for efficiency
- The function only processes non-join qualifications since join conditions inherently fail for unmatched tuples
- Critical for outer join correctness, ensuring SQL standard compliance for LEFT and FULL OUTER JOINs
- Part of the merge join state machine that handles different join scenarios
- Includes debug output via MJ_printf for development and troubleshooting
- Performance instrumentation tracks filtered tuples for query optimization feedback