# LockRowsState

## Location
[src/include/nodes/execnodes.h:2805-2810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2805-L2810)

## Overview
LockRowsState is the execution state structure for LockRows nodes in PostgreSQL's executor, used to enforce row-level locking for FOR UPDATE, FOR KEY UPDATE, FOR SHARE, and FOR KEY SHARE clauses.

## Definition

```c
typedef struct LockRowsState
{
	PlanState	ps;				/* its first field is NodeTag */
	List	   *lr_arowMarks;	/* List of ExecAuxRowMarks */
	EPQState	lr_epqstate;	/* for evaluating EvalPlanQual rechecks */
} LockRowsState;
```
## Detailed Description
LockRowsState manages the execution state for LockRows nodes, which implement row-level locking semantics in PostgreSQL. These nodes are inserted into the execution plan when queries contain FOR UPDATE, FOR KEY UPDATE, FOR SHARE, or FOR KEY SHARE clauses. The structure maintains row marks for tracking which rows need to be locked and includes EvalPlanQual (EPQ) state for handling concurrent modifications during lock acquisition.

## Parameters / Member Variables
- `ps`: Base PlanState structure containing common executor node information
- `lr_arowMarks`: List of ExecAuxRowMark structures that track row locking information for each relation involved in the locking operation
- `lr_epqstate`: EvalPlanQual state used for re-evaluating plan conditions when concurrent row modifications are detected during lock acquisition

## Dependencies
- Functions called/Symbols referenced:
  - [EPQState](../E/EPQState.md)
- Called from (representative examples):
  - [ExecLockRows](../E/ExecLockRows.md)
  - [ExecInitLockRows](../E/ExecInitLockRows.md)
  - [ExecEndLockRows](../E/ExecEndLockRows.md)
  - [ExecReScanLockRows](../E/ExecReScanLockRows.md)

## Notes and Other Information
- Essential for implementing PostgreSQL's row-level locking semantics in SELECT FOR UPDATE/SHARE queries
- The EPQ mechanism allows the system to handle concurrent updates properly by re-evaluating conditions after lock acquisition
- Row marks track the specific type of lock required (UPDATE, KEY UPDATE, SHARE, KEY SHARE) for each relation
- Coordinates with the buffer manager and lock manager to ensure proper isolation levels are maintained