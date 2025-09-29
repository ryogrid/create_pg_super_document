# ExecEndLockRows

## Location
[src/backend/executor/nodeLockRows.c:385-393](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeLockRows.c#L385-L393)

## Overview
ExecEndLockRows shuts down the LockRows node by cleaning up EPQ state and recursively ending the outer subplan.

## Definition
void ExecEndLockRows(LockRowsState *node)

## Detailed Description
ExecEndLockRows performs cleanup operations when a LockRows node finishes execution or is being destroyed. The function performs two main cleanup tasks:

1. Ends the EvalPlanQual state associated with the node, releasing any resources held by the EPQ machinery
2. Recursively calls ExecEndNode on the outer subplan to ensure proper cleanup of the entire plan subtree

The function is designed to be safe to call multiple times, as EvalPlanQualEnd can handle being called on already-ended EPQ state without harm.

## Parameters / Member Variables
- node: Pointer to the LockRowsState structure to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [EvalPlanQualEnd](EvalPlanQualEnd.md) (to cleanup EPQ state)
  - [ExecEndNode](ExecEndNode.md) (to recursively end outer subplan)
  - outerPlanState (macro to access outer plan state)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (standard plan node cleanup)

## Notes and Other Information
- The function safely handles multiple calls to EvalPlanQualEnd
- Cleanup is performed in reverse order of initialization
- Part of the standard PostgreSQL plan node lifecycle (Init, Exec, End)
- Function is located at src/backend/executor/nodeLockRows.c:385-393

## Simplified Source

```c
void ExecEndLockRows(LockRowsState *node) {
    // Clean up EvalPlanQual state (safe to call multiple times)
    EvalPlanQualEnd(&node->lr_epqstate);

    // Recursively shut down the outer subplan
    ExecEndNode(outerPlanState(node));
}
```