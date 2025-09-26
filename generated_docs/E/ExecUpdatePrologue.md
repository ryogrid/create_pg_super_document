# ExecUpdatePrologue

## Location
[src/backend/executor/nodeModifyTable.c:1924-1966](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L1924-L1966)

## Overview
A subroutine for ExecUpdate that prepares the executor state for UPDATE operations, including materializing slots, opening indexes, and executing BEFORE ROW triggers.

## Definition

```c
static bool
ExecUpdatePrologue(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
				   ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot *slot,
				   TM_Result *result)
```
## Detailed Description
ExecUpdatePrologue performs the preparatory steps required before executing an UPDATE operation. Its primary responsibilities include:

1. **Slot Materialization**: Ensures the new tuple data in the slot is materialized and ready for processing
2. **Index Management**: Opens the table's indexes if not already done, preparing for index entry updates
3. **BEFORE Trigger Execution**: Fires BEFORE ROW UPDATE triggers, which may modify the tuple data or cancel the update operation
4. **Pending Insert Processing**: Flushes any pending bulk inserts to ensure data visibility for triggers

The function serves as a critical preparation phase that ensures all prerequisites are met before the actual tuple update occurs. BEFORE triggers have the opportunity to examine and modify both the old and new tuple data, and can prevent the update by returning false.

## Parameters / Member Variables
- : ModifyTableContext containing the execution state and metadata for the modify operation
- : Information about the result relation being updated
- : ItemPointer identifying the physical location of the tuple to be updated
- : HeapTuple containing the current tuple data before update
- : TupleTableSlot containing the new tuple data after update
- : Output parameter receiving the tuple modification result status

## Dependencies
- Functions called/Symbols referenced:
  - [ExecMaterializeSlot](ExecMaterializeSlot.md) (ensures slot data is materialized)
  - [ExecOpenIndices](ExecOpenIndices.md) (opens table indexes for index entry updates)
  - [ExecPendingInserts](ExecPendingInserts.md) (flushes pending bulk inserts)
  - [ExecBRUpdateTriggersNew](ExecBRUpdateTriggersNew.md) (executes BEFORE ROW UPDATE triggers)
  - TM_Ok (tuple modification result constant)
  - CMD_MERGE (command type constant for MERGE operations)
- Called from:
  - [ExecUpdate](ExecUpdate.md) (main update execution function)
  - [ExecMergeMatched](ExecMergeMatched.md) (MERGE statement matched case execution)

## Notes and Other Information
- This is a static function, only accessible within nodeModifyTable.c
- Returns false if BEFORE ROW triggers cancel the update, true otherwise
- Initializes result parameter to TM_Ok by default
- Opens indexes lazily only when needed (table has indexes and they're not already open)
- Handles special logic for MERGE operations in trigger execution
- Ensures pending inserts are flushed before trigger execution to maintain data visibility
- Part of PostgreSQL's execution engine for DML operations, specifically the UPDATE workflow