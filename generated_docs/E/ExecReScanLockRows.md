# ExecReScanLockRows

## Location
src/backend/executor/nodeLockRows.c: 394 - 404

## Overview
ExecReScanLockRows resets the LockRows node to restart execution from the beginning by conditionally rescanning its outer subplan.

## Definition
void ExecReScanLockRows(LockRowsState *node)

## Detailed Description
ExecReScanLockRows implements the rescan functionality for LockRows nodes, which is called when the execution needs to restart from the beginning (typically in nested loop joins or subquery re-execution scenarios). The function uses PostgreSQL standard rescan optimization:

- It checks if the outer subplan has any parameter changes (chgParam)
- If there are no parameter changes, it explicitly calls ExecReScan on the outer plan
- If parameters have changed, the outer plan will be automatically rescanned on the next ExecProcNode call

This optimization avoids unnecessary work when the outer plan will be rescanned anyway due to parameter changes.

## Parameters / Member Variables
- node: Pointer to the LockRowsState structure to be rescanned

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState (macro to access outer plan state)
  - ExecReScan (to rescan outer subplan when needed)
- Called from (representative examples):
  - ExecReScan (standard plan node rescan mechanism)

## Notes and Other Information
- The rescan optimization based on chgParam is a common pattern across PostgreSQL executor nodes
- LockRows nodes do not maintain their own state that needs resetting, only the outer subplan
- The function follows the standard PostgreSQL executor rescan protocol
- Function is located at src/backend/executor/nodeLockRows.c:394-404