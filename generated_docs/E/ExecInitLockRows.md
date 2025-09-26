# ExecInitLockRows

## Location
[src/backend/executor/nodeLockRows.c:291-384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeLockRows.c#L291-L384)

## Overview
ExecInitLockRows initializes the LockRows node state structures and its subplan, setting up row marking infrastructure for tuple locking operations.

## Definition
LockRowsState *ExecInitLockRows(LockRows *node, EState *estate, int eflags)

## Detailed Description
ExecInitLockRows performs initialization tasks for the LockRows execution node. It creates and configures the LockRowsState structure, initializes the outer subplan, and sets up the row marking infrastructure needed for tuple locking. Key initialization steps include:

- Creating the LockRowsState structure and linking it with the plan node
- Setting the execution function pointer to ExecLockRows
- Initializing result tuple type information from the target list
- Initializing the outer subplan recursively
- Building ExecAuxRowMark structures for each PlanRowMark
- Separating locking row marks from non-locking marks for EPQ processing
- Initializing the EvalPlanQual state for handling concurrent updates

The function distinguishes between row marks that require actual tuple locking versus those used only for EPQ testing, optimizing performance by avoiding unnecessary lock attempts.

## Parameters / Member Variables
- node: Pointer to the LockRows plan node containing row marking specifications
- estate: Executor state containing transaction and snapshot information
- eflags: Execution flags controlling initialization behavior

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create LockRowsState)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md) (to initialize result tuple type)
  - [ExecInitNode](ExecInitNode.md) (to initialize outer subplan)
  - [ExecGetResultSlotOps](ExecGetResultSlotOps.md) (to set up result slot operations)
  - [ExecFindRowMark](ExecFindRowMark.md) (to locate row mark in estate)
  - [ExecBuildAuxRowMark](ExecBuildAuxRowMark.md) (to create auxiliary row mark structure)
  - RowMarkRequiresRowShareLock (to determine if locking is needed)
  - [EvalPlanQualInit](EvalPlanQualInit.md) (to initialize EPQ state)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (standard plan node initialization)

## Notes and Other Information
- The function asserts that EXEC_FLAG_MARK is not set as LockRows nodes do not support mark/restore
- [LockRows](../L/LockRows.md) nodes do not perform projections, so ps_ProjInfo is set to NULL
- Row marks are categorized into locking marks (added to lr_arowMarks) and non-locking marks (passed to EPQ)
- Parent row marks are ignored at runtime as they are only used during planning
- The node reuses result slot operations from its outer subplan for efficiency
- Function is located at src/backend/executor/nodeLockRows.c:291-384