# ExecResult

## Location
src/backend/executor/nodeResult.c: 67 - 145

## Overview
ExecResult is the core execution function for the Result plan node that returns tuples from an outer plan or generates tuples from constant expressions, applying optional qualification clauses.

## Definition


## Detailed Description
ExecResult processes Result plan nodes, which are used in two primary scenarios:
1. **With outer plan**: Acts as a filter/projection layer that retrieves tuples from an outer plan and applies qualifications
2. **Without outer plan**: Generates result tuples from constant target lists (e.g., SELECT 1, 2, 3)

The function implements an optimization for constant qualifications by checking them only once at the beginning. If a constant qualification like (2 > 1) fails, the node is marked as done and no further processing occurs.

For nodes with outer plans, it continuously retrieves tuples from the outer plan until exhausted. For nodes without outer plans (constant result generation), it produces exactly one tuple and marks itself as done.

The function uses projection to transform input tuples or generate output tuples according to the target list specified in the Result node.

## Parameters / Member Variables
- : The PlanState containing execution state for this Result node, cast to ResultState internally

## Dependencies
- Functions called/Symbols referenced:
  - castNode (for casting PlanState to ResultState)
  - CHECK_FOR_INTERRUPTS (interrupt handling macro)
  - ExecQual (for evaluating qualification expressions)
  - ResetExprContext (to reset per-tuple memory context)
  - outerPlanState (to get the outer plan state)
  - ExecProcNode (to execute the outer plan)
  - TupIsNull (to check if tuple is null)
  - ExecProject (to perform projection and generate result tuples)
- Called from:
  - [ExecInitResult](ExecInitResult.md) (sets this as the execution function during initialization)

## Notes and Other Information
- [Result](../R/Result.md) nodes with right subtrees are never planned in PostgreSQL, so the right subtree is ignored
- The rs_done flag prevents redundant execution for constant result generation
- The rs_checkqual flag ensures constant qualifications are evaluated only once
- Memory context is reset between tuple cycles to prevent memory leaks during expression evaluation
- The function returns NULL when no more tuples are available or when constant qualifications fail