# ExecAssignExprContext

## Location
[src/backend/executor/execUtils.c:483-492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L483-L492)

## Overview
Initializes the ps_ExprContext field of a PlanState by creating and assigning a new ExprContext for expression evaluation.

## Definition

```c
void
ExecAssignExprContext(EState *estate, PlanState *planstate)
```
## Detailed Description
ExecAssignExprContext is a utility function that initializes the expression context field (ps_ExprContext) of a PlanState node. This function is essential for nodes that need to evaluate expressions using ExecQual or ExecProject routines, as these functions require an active expression context to operate.

The function creates a new ExprContext using CreateExprContext and assigns it to the planstate's ps_ExprContext field. This provides the plan node with the necessary infrastructure for expression evaluation, including memory management for per-tuple allocations and storage for evaluation state.

This initialization is only necessary for plan nodes that actually evaluate expressions. Nodes that don't perform expression evaluation (such as pure data flow nodes) don't need to call this function.

## Parameters / Member Variables
- : The EState structure that will own the created ExprContext
- : The PlanState node that will receive the ExprContext in its ps_ExprContext field

## Dependencies
- Functions called/Symbols referenced:
  - [CreateExprContext](../C/CreateExprContext.md) (creates the ExprContext and associates it with the EState)

- Called from (representative examples):
  - [ExecInitPartitionPruning](ExecInitPartitionPruning.md) (in src/backend/executor/execPartition.c:1810)
  - [ExecInitAgg](ExecInitAgg.md) (in src/backend/executor/nodeAgg.c:3282, 3287, 3294)
  - [ExecInitBitmapHeapScan](ExecInitBitmapHeapScan.md) (in src/backend/executor/nodeBitmapHeapscan.c:726)
  - [ExecInitBitmapIndexScan](ExecInitBitmapIndexScan.md) (in src/backend/executor/nodeBitmapIndexscan.c:290)
  - [ExecInitSeqScan](ExecInitSeqScan.md) (in src/backend/executor/nodeSeqscan.c:147)
  - [ExecInitResult](ExecInitResult.md) (in src/backend/executor/nodeResult.c:204)
  - [ExecInitHashJoin](ExecInitHashJoin.md) (in src/backend/executor/nodeHashjoin.c:742)
  - [ExecInitModifyTable](ExecInitModifyTable.md) (in src/backend/executor/nodeModifyTable.c:4690, 4741)
  - Many other ExecInit* functions across executor nodes

## Notes and Other Information
- This function is expected to be called with CurrentMemoryContext equal to the per-query memory context
- Essential for any plan node that uses ExecQual or ExecProject for expression evaluation
- Part of the standard initialization pattern for PostgreSQL executor nodes
- The created ExprContext will be automatically cleaned up when the EState is freed
- Not needed for nodes that don't evaluate expressions (pure data flow operations)
- Commonly called during the ExecInit phase of plan node initialization
- The ps_ExprContext field becomes the node's primary context for expression evaluation throughout its execution lifecycle

## Simplified Source

```c
void ExecAssignExprContext(EState *estate, PlanState *planstate) {
    // Create and assign expression context for nodes that evaluate expressions
    planstate->ps_ExprContext = CreateExprContext(estate);
}
```