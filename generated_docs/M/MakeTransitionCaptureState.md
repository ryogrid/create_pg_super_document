# MakeTransitionCaptureState

## Location
[src/backend/commands/trigger.c:4969-5072](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L4969-L5072)

## Overview
Creates a TransitionCaptureState object that holds all state needed to decide whether to capture tuples in transition tables for trigger processing, based on trigger requirements and operation type.

## Definition
```c
TransitionCaptureState *MakeTransitionCaptureState(TriggerDesc *trigdesc, 
                                                    Oid relid, 
                                                    CmdType cmdType)
```

## Detailed Description
This function is central to PostgreSQL's transition table functionality, which allows AFTER triggers to access OLD and NEW table references containing all affected rows. It analyzes the provided trigger descriptor to determine which transition tables are needed based on the command type and trigger definitions.

The function implements SQL standard semantics where all operations of the same kind on the same table during one query share a single transition table. It creates or reuses tuplestores managed by AfterTriggersTableData structures, which are indexed by table OID and command type.

For different command types, the function determines the appropriate combination of transition tables: INSERT operations may need NEW tables, DELETE operations may need OLD tables, UPDATE operations may need both OLD and NEW tables, and MERGE operations may need all combinations depending on the specific triggers defined.

The function ensures proper resource management by creating tuplestores in the current transaction context and associating them with the transaction's resource owner, ensuring they are cleaned up appropriately when the transaction ends.

## Parameters / Member Variables
- `trigdesc`: Pointer to TriggerDesc containing trigger definitions that specify which transition tables are needed
- `relid`: OID of the target relation for which transition capture state is being created
- `cmdType`: Command type (CMD_INSERT, CMD_UPDATE, CMD_DELETE, or CMD_MERGE) that determines which transition tables are relevant

## Dependencies
- Functions called/Symbols referenced:
  - [GetAfterTriggersTableData](../G/GetAfterTriggersTableData.md)
  - [AfterTriggerEnlargeQueryState](../A/AfterTriggerEnlargeQueryState.md)
  - [tuplestore_begin_heap](../t/tuplestore_begin_heap.md)
  - [palloc0](../p/palloc0.md)
  - [MemoryContextSwitchTo](MemoryContextSwitchTo.md)
  - elog
- Called from (representative examples):
  - [ExecSetupTransitionCaptureState](../E/ExecSetupTransitionCaptureState.md)
  - [CopyFrom](../C/CopyFrom.md)

## Notes and Other Information
- Returns NULL if no transition tables are needed for the given triggers and command type
- Creates tuplestores lazily - only creates those actually needed by the trigger definitions
- Uses work_mem for tuplestore size limits and creates them as non-randomaccess, non-forward-only stores
- Handles all SQL DML command types: INSERT, UPDATE, DELETE, and MERGE
- Ensures proper memory context and resource owner management for tuplestore lifecycle
- The resulting state can be used with ExecAR* functions during query execution
- Supports inheritance scenarios by allowing tcs_original_insert_tuple to be set for child table handling
- Part of PostgreSQL's implementation of SQL standard transition table functionality for triggers
- Validates query depth to ensure it's called within proper query execution context