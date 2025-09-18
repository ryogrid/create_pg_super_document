# MJFillInner

## Location
src/backend/executor/nodeMergejoin.c: 483 - 518

## Overview
MJFillInner generates a fake join tuple with nulls for the outer tuple and returns it if it passes the non-join qualification clauses, used in merge join operations for right outer joins.

## Definition


## Detailed Description
MJFillInner is a specialized function in the merge join executor that handles the generation of result tuples when performing right outer joins. When the merge join needs to produce output tuples for inner side tuples that have no matching outer side tuples, this function creates a "fill" tuple by combining the current inner tuple with a null outer tuple.

The function sets up the expression context with the null outer tuple slot and the current inner tuple, then evaluates any non-join qualification clauses. If these qualifications pass, it projects the result tuple and returns it. This ensures that right outer joins correctly include all inner tuples even when they don't have matching outer tuples.

## Parameters / Member Variables
- : MergeJoinState containing the merge join execution state, including tuple slots, expression context, and projection information

## Dependencies
- Functions called/Symbols referenced:
  - [MergeJoinState](MergeJoinState.md) (data structure)
  - ResetExprContext (resets expression evaluation context)
  - ExecQual (evaluates qualification expressions)
  - MJ_printf (debug logging macro)
  - ExecProject (performs tuple projection)
  - InstrCountFiltered2 (instrumentation for filtered tuples)
- Called from (representative examples):
  - ExecMergeJoin (main merge join execution function, called at multiple points during right outer join processing)

## Notes and Other Information
- This function is part of the merge join executor's handling of outer join semantics
- It's specifically used for right outer joins to ensure all inner tuples are included in the result
- The function only returns a tuple if the non-join quals are satisfied, otherwise it returns NULL
- Debug output is conditionally compiled based on MJ_printf macro
- Instrumentation is included to track filtered tuples for performance monitoring
- The function uses the pre-allocated null outer tuple slot for efficiency