# ExecOnConflictUpdate

## Location
[src/backend/executor/nodeModifyTable.c:2544-2763](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L2544-L2763)

## Overview
ExecOnConflictUpdate handles the UPDATE portion of INSERT ON CONFLICT DO UPDATE commands by attempting to lock and update the conflicting tuple if the ON CONFLICT condition is satisfied.

## Definition

```c
static bool
ExecOnConflictUpdate(ModifyTableContext *context,
					 ResultRelInfo *resultRelInfo,
					 ItemPointer conflictTid,
					 TupleTableSlot *excludedSlot,
					 bool canSetTag,
					 TupleTableSlot **returning)
```
## Detailed Description
ExecOnConflictUpdate implements the core logic for INSERT ON CONFLICT DO UPDATE operations. The function performs several critical steps:

1. **Tuple Locking**: Attempts to lock the conflicting tuple for update using table_tuple_lock with appropriate isolation handling
2. **Concurrency Control**: Handles various tuple states (TM_Ok, TM_Invisible, TM_Updated, TM_Deleted, TM_SelfModified) that may occur due to concurrent operations
3. **Visibility Checking**: Ensures the tuple is visible according to the current transaction's MVCC snapshot requirements
4. **Condition Evaluation**: Sets up expression context with EXCLUDED tuple as inner tuple and existing tuple as scan tuple, then evaluates the ON CONFLICT WHERE clause
5. **Security Validation**: Applies Row-Level Security (RLS) conflict checks using WCO_RLS_CONFLICT_CHECK
6. **Projection and Update**: Projects the new tuple values and delegates to ExecUpdate for the actual update operation

The function returns true if processing is complete (with or without an update), or false if the caller should retry the entire INSERT operation from scratch due to concurrency conflicts.

## Parameters / Member Variables
- : ModifyTableContext containing execution state and metadata
- : ResultRelInfo for the target relation being updated
- : ItemPointer identifying the conflicting tuple that needs to be updated
- : TupleTableSlot containing the values from the conflicting INSERT (EXCLUDED tuple)
- : Boolean indicating whether command tags can be set
- : Pointer to TupleTableSlot pointer for storing RETURNING clause results

## Dependencies
- Functions called/Symbols referenced:
  - [ExecUpdateLockMode](ExecUpdateLockMode.md)
  - [table_tuple_lock](../t/table_tuple_lock.md)
  - [slot_getsysattr](../s/slot_getsysattr.md)
  - [DatumGetTransactionId](../D/DatumGetTransactionId.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - [ItemPointerIndicatesMovedPartitions](../I/ItemPointerIndicatesMovedPartitions.md)
  - [ExecClearTuple](ExecClearTuple.md)
  - [ExecCheckTupleVisible](ExecCheckTupleVisible.md)
  - [ExecQual](ExecQual.md)
  - [ExecWithCheckOptions](ExecWithCheckOptions.md)
  - [ExecProject](ExecProject.md)
  - [ExecUpdate](ExecUpdate.md)
  - InstrCountFiltered1
  - IsolationUsesXactSnapshot
- Called from (representative examples):
  - [ExecInsert](ExecInsert.md) (src/backend/executor/nodeModifyTable.c:1078)

## Notes and Other Information
- The function is static and only used within nodeModifyTable.c for ON CONFLICT DO UPDATE processing
- The function includes sophisticated concurrency handling, especially for the TM_Invisible case which prevents duplicate updates of the same row within a single command
- Cross-partition updates are not supported for ON CONFLICT DO UPDATE operations, as indicated by the assertion checking for moved partitions
- The EXCLUDED tuple is made available as the inner tuple in the expression context, allowing ON CONFLICT SET clauses to reference both old and new values
- RLS (Row-Level Security) checks are specifically handled using WCO_RLS_CONFLICT_CHECK to ensure security policies are enforced during conflict resolution
- The function handles the case where the target tuple might be modified during the execution of the ON CONFLICT clause itself
- System relations are blocked from using ON CONFLICT operations at the parser level, so no special handling is needed for tuple locking tags
- Memory management includes clearing the existing tuple slot to prevent resource leaks between conflicts