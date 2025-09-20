# ExecARDeleteTriggers

## Location
[src/backend/commands/trigger.c:2812-2858](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2812-L2858)

## Overview
ExecARDeleteTriggers executes AFTER ROW DELETE triggers and handles transition table capture for DELETE operations, including support for cross-partition updates.

## Definition

```c
void
ExecARDeleteTriggers(EState *estate,
					 ResultRelInfo *relinfo,
					 ItemPointer tupleid,
					 HeapTuple fdw_trigtuple,
					 TransitionCaptureState *transition_capture,
					 bool is_crosspart_update)
```
## Detailed Description
This function is responsible for executing AFTER ROW DELETE triggers and managing transition table capture after a row has been successfully deleted. Unlike BEFORE triggers, AFTER triggers cannot prevent the delete operation but can perform side effects based on the deleted data.

The function first checks if there are any AFTER DELETE triggers to execute or transition tables to populate. If so, it retrieves the deleted tuple (either from the provided fdw_trigtuple for foreign tables or by fetching it using the tupleid for regular tables) and stores it in a tuple slot. The actual trigger execution is deferred through the AfterTriggerSaveEvent mechanism, which ensures proper ordering and execution of AFTER triggers.

The function includes special handling for foreign data wrapper tables, prohibiting transition table capture from child foreign tables as this feature is not supported.

## Parameters / Member Variables
- : Executor state containing execution context and memory management information
- : ResultRelInfo containing relation metadata and trigger information
- : ItemPointer identifying the deleted tuple (used for regular tables)
- : HeapTuple for foreign data wrapper tables (alternative to tupleid)
- : State for capturing tuples into OLD transition tables
- : Boolean flag indicating if the DELETE is part of a cross-partition update operation

## Dependencies
- Functions called/Symbols referenced:
  - [ExecGetTriggerOldSlot](ExecGetTriggerOldSlot.md)
  - [GetTupleForTrigger](../G/GetTupleForTrigger.md)
  - [ExecForceStoreHeapTuple](ExecForceStoreHeapTuple.md)
  - AfterTriggerSaveEvent
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
- Data types referenced:
  - TransitionCaptureState
  - TriggerDesc
  - LockTupleExclusive
  - TRIGGER_EVENT_DELETE
- Called from (representative examples):
  - [ExecSimpleRelationDelete](ExecSimpleRelationDelete.md)
  - [ExecDeleteEpilogue](ExecDeleteEpilogue.md)

## Notes and Other Information
- This function does not directly execute triggers but rather queues them for execution via AfterTriggerSaveEvent
- Includes error handling for unsupported transition table capture from child foreign tables
- The is_crosspart_update parameter affects how the trigger event is processed in the after-trigger system
- Returns void as AFTER triggers cannot prevent the delete operation
- Located in src/backend/commands/trigger.c:2812-2858
- Part of PostgreSQL's deferred trigger execution system for DELETE operations