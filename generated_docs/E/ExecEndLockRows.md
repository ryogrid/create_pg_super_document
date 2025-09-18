# ExecEndLockRows

## Location
src/backend/executor/nodeLockRows.c: 385 - 393

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
  - EvalPlanQualEnd (to cleanup EPQ state)
  - ExecEndNode (to recursively end outer subplan)
  - outerPlanState (macro to access outer plan state)
- Called from (representative examples):
  - ExecEndNode (standard plan node cleanup)

## Notes and Other Information
- The function safely handles multiple calls to EvalPlanQualEnd
- Cleanup is performed in reverse order of initialization
- Part of the standard PostgreSQL plan node lifecycle (Init, Exec, End)
- Function is located at src/backend/executor/nodeLockRows.c:385-393