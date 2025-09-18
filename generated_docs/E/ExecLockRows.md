# ExecLockRows

## Location
[src/backend/executor/nodeLockRows.c:38-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeLockRows.c#L38-L290)

## Overview
ExecLockRows is the main execution function for the LockRows plan node that attempts to lock tuples retrieved from its subplan, handling various locking modes and foreign table scenarios.

## Definition
static TupleTableSlot *ExecLockRows(PlanState *pstate)

## Detailed Description
ExecLockRows implements tuple locking functionality in PostgreSQL execution engine. It retrieves tuples from its outer subplan and attempts to lock them according to the specified row marking requirements. The function handles multiple locking scenarios including:

- Regular table tuple locking with different lock modes (SHARE, KEY SHARE, EXCLUSIVE, NO KEY EXCLUSIVE)
- Foreign table locking through FDW interfaces
- EvalPlanQual (EPQ) processing for concurrent update scenarios
- Lock conflict resolution and retry logic
- Serialization failure handling in isolation levels that require it

The function processes each tuple by iterating through all row marks associated with the LockRows node, attempting to acquire the appropriate lock for each marked relation. If locking succeeds and no concurrent updates require EPQ reprocessing, the locked tuple is returned.

## Parameters / Member Variables
- pstate: Pointer to the PlanState structure, cast to LockRowsState internally

## Dependencies
- Functions called/Symbols referenced:
  - ExecProcNode (to get tuples from outer plan)
  - TupIsNull (to check for null tuples)
  - [EvalPlanQualEnd](EvalPlanQualEnd.md)/Begin/SetSlot/Next (EPQ machinery)
  - ExecGetJunkAttribute (to extract ctid and tableoid)
  - [GetFdwRoutineForRelation](../G/GetFdwRoutineForRelation.md) (for foreign table operations)
  - table_tuple_lock (core tuple locking function)
  - IsolationUsesXactSnapshot (isolation level checking)
- Called from (representative examples):
  - [ExecInitLockRows](ExecInitLockRows.md) (sets this as the ExecProcNode function)

## Notes and Other Information
- The function uses a goto label lnext for retry logic when tuples cannot be locked or fail EPQ checks
- Handles the Halloween problem by ignoring self-modified tuples
- Foreign table locking requires FDW to implement RefetchForeignRow callback
- EPQ (EvalPlanQual) testing is triggered when tuples are updated during locking process
- Different lock modes correspond to SQL row locking clauses (FOR SHARE, FOR UPDATE, etc.)
- Function is located at src/backend/executor/nodeLockRows.c:38-290