# MJFillInner

## Location
[src/backend/executor/nodeMergejoin.c:483-518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L483-L518)

## Overview
MJFillInner generates a fake join tuple with nulls for the outer tuple and returns it if it passes the non-join qualification clauses, used in merge join operations for right outer joins.

## Definition

```c
static TupleTableSlot *
MJFillInner(MergeJoinState *node)
```
## Detailed Description
MJFillInner is a specialized function in the merge join executor that handles the generation of result tuples when performing right outer joins. When the merge join needs to produce output tuples for inner side tuples that have no matching outer side tuples, this function creates a "fill" tuple by combining the current inner tuple with a null outer tuple.

The function sets up the expression context with the null outer tuple slot and the current inner tuple, then evaluates any non-join qualification clauses. If these qualifications pass, it projects the result tuple and returns it. This ensures that right outer joins correctly include all inner tuples even when they don't have matching outer tuples.

## Parameters / Member Variables
- `*node`: MergeJoinState containing the merge join execution state, including tuple slots, expression context, and projection information
## Dependencies
- Functions called/Symbols referenced:
  - [MergeJoinState](MergeJoinState.md) (data structure)
  - ResetExprContext (resets expression evaluation context)
  - [ExecQual](../E/ExecQual.md) (evaluates qualification expressions)
  - MJ_printf (debug logging macro)
  - [ExecProject](../E/ExecProject.md) (performs tuple projection)
  - InstrCountFiltered2 (instrumentation for filtered tuples)
- Called from (representative examples):
  - [ExecMergeJoin](../E/ExecMergeJoin.md) (main merge join execution function, called at multiple points during right outer join processing)

## Notes and Other Information
- This function is part of the merge join executor's handling of outer join semantics
- It's specifically used for right outer joins to ensure all inner tuples are included in the result
- The function only returns a tuple if the non-join quals are satisfied, otherwise it returns NULL
- Debug output is conditionally compiled based on MJ_printf macro
- [Instrumentation](../I/Instrumentation.md) is included to track filtered tuples for performance monitoring
- The function uses the pre-allocated null outer tuple slot for efficiency

## Simplified Source

```c
static TupleTableSlot *
MJFillInner(MergeJoinState *node) {
    ExprContext *econtext = node->js.ps.ps_ExprContext;
    ExprState *otherqual = node->js.ps.qual;

    // Reset context for new evaluation
    ResetExprContext(econtext);

    // Set up fake join tuple: NULL outer + current inner
    econtext->ecxt_outertuple = node->mj_NullOuterTupleSlot;
    econtext->ecxt_innertuple = node->mj_InnerTupleSlot;

    // Check if non-join quals are satisfied
    if (ExecQual(otherqual, econtext)) {
        // Quals passed - project and return the result tuple
        return ExecProject(node->js.ps.ps_ProjInfo);
    } else {
        // Quals failed - count as filtered for instrumentation
        InstrCountFiltered2(node, 1);
    }

    return NULL;
}
```