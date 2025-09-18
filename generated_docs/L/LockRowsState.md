# LockRowsState

## Location
src/include/nodes/execnodes.h: 2805 - 2810

## Overview
LockRowsState is the execution state structure for LockRows nodes in PostgreSQL's executor, used to enforce row-level locking for FOR UPDATE, FOR KEY UPDATE, FOR SHARE, and FOR KEY SHARE clauses.

## Definition


## Detailed Description
LockRowsState manages the execution state for LockRows nodes, which implement row-level locking semantics in PostgreSQL. These nodes are inserted into the execution plan when queries contain FOR UPDATE, FOR KEY UPDATE, FOR SHARE, or FOR KEY SHARE clauses. The structure maintains row marks for tracking which rows need to be locked and includes EvalPlanQual (EPQ) state for handling concurrent modifications during lock acquisition.

## Parameters / Member Variables
-   PID TTY          TIME CMD
16345 ?        00:00:00 bash
16372 ?        00:00:00 ps
21784 ?        00:00:00 dbus-daemon: Base PlanState structure containing common executor node information
- : List of ExecAuxRowMark structures that track row locking information for each relation involved in the locking operation
- : EvalPlanQual state used for re-evaluating plan conditions when concurrent row modifications are detected during lock acquisition

## Dependencies
- Functions called/Symbols referenced:
  - EPQState
- Called from (representative examples):
  - ExecLockRows
  - ExecInitLockRows
  - ExecEndLockRows
  - ExecReScanLockRows

## Notes and Other Information
- Essential for implementing PostgreSQL's row-level locking semantics in SELECT FOR UPDATE/SHARE queries
- The EPQ mechanism allows the system to handle concurrent updates properly by re-evaluating conditions after lock acquisition
- Row marks track the specific type of lock required (UPDATE, KEY UPDATE, SHARE, KEY SHARE) for each relation
- Coordinates with the buffer manager and lock manager to ensure proper isolation levels are maintained