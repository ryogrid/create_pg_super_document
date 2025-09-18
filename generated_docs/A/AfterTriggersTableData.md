# AfterTriggersTableData

## Location
[src/backend/commands/trigger.c:3936-4004](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L3936-L4004)

## Overview
AfterTriggersTableData is a structure that maintains per-table state for managing after-triggers and transition tables during SQL statement execution in PostgreSQL.

## Definition


## Detailed Description
AfterTriggersTableData serves as the central data structure for managing after-trigger execution and transition table maintenance on a per-table, per-operation basis. Each instance represents a unique combination of table (relid) and command type (INSERT/UPDATE/DELETE), providing isolated storage for trigger events and transition tables.

The structure supports PostgreSQL's MERGE statement by maintaining separate transition tables for each DML operation type, allowing a single MERGE to perform INSERT, UPDATE, and DELETE operations while properly tracking their respective transition tables. The design ensures that trigger execution state is properly maintained across complex multi-operation statements.

The structure is allocated in the current transaction's memory context and exists for the duration of trigger processing, being cleaned up after trigger execution completes.

## Parameters / Member Variables
- : Object ID of the target table for which triggers are being managed
- : The type of DML operation (CMD_INSERT, CMD_UPDATE, or CMD_DELETE) 
- : Boolean flag indicating whether this structure is closed to new tuple additions
- : Flag tracking whether BEFORE statement triggers have been queued for execution
- : Flag tracking whether AFTER statement triggers have been queued for execution
- : Saved pointer to the list of after-trigger events when triggers have been queued
- : Tuplestore containing the "old" transition table for UPDATE operations
- : Tuplestore containing the "new" transition table for UPDATE operations
- : Tuplestore containing the "old" transition table for DELETE operations
- : Tuplestore containing the "new" transition table for INSERT operations
- : TupleTableSlot used for converting tuples to the tuplestore's internal format

## Dependencies
- Functions called/Symbols referenced:
  - CmdType (enum for command types)
  - [AfterTriggerEventList](AfterTriggerEventList.md) (structure for event lists)
  - Tuplestorestate (tuplestore management)
  - TupleTableSlot (tuple slot management)
  - [GetAfterTriggersTableData](../G/GetAfterTriggersTableData.md) (factory function)
  - [GetAfterTriggersStoreSlot](../G/GetAfterTriggersStoreSlot.md) (slot accessor)
  - [GetAfterTriggersTransitionTable](../G/GetAfterTriggersTransitionTable.md) (transition table accessor)
  - [TransitionTableAddTuple](../T/TransitionTableAddTuple.md) (tuple addition to transition tables)

- Called from (representative examples):
  - [GetAfterTriggersTableData](../G/GetAfterTriggersTableData.md) (factory/lookup function)
  - [afterTriggerInvokeEvents](../a/afterTriggerInvokeEvents.md) (trigger execution)
  - [MakeTransitionCaptureState](../M/MakeTransitionCaptureState.md) (transition table setup)
  - [TransitionTableAddTuple](../T/TransitionTableAddTuple.md) (tuple storage)
  - [AfterTriggerFreeQuery](AfterTriggerFreeQuery.md) (cleanup)

## Notes and Other Information
- The structure is designed to handle PostgreSQL's MERGE statement which can perform multiple DML operations in a single statement, requiring separate transition tables for each operation type
- Memory allocation occurs in CurTransactionContext, ensuring proper cleanup at transaction end
- The lookup key combination of relid + cmdType ensures that each table/operation pair gets its own isolated trigger management state
- Transition tables are only created when actually needed by triggers that reference OLD or NEW transition tables
- The 'closed' flag prevents additional tuples from being added to transition tables after certain points in trigger processing