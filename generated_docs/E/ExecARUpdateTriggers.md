# ExecARUpdateTriggers

## Location
[src/backend/commands/trigger.c:3171-3240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L3171-L3240)

## Overview
Executes AFTER ROW UPDATE triggers and captures transition table data for UPDATE operations, with special support for cross-partition updates and foreign table constraints.

## Definition

```c
void
ExecARUpdateTriggers(EState *estate, ResultRelInfo *relinfo,
					 ResultRelInfo *src_partinfo,
					 ResultRelInfo *dst_partinfo,
					 ItemPointer tupleid,
					 HeapTuple fdw_trigtuple,
					 TupleTableSlot *newslot,
					 List *recheckIndexes,
					 TransitionCaptureState *transition_capture,
					 bool is_crosspart_update)
```
## Detailed Description
This function handles the execution of AFTER ROW UPDATE triggers and transition table capture for UPDATE operations. It supports complex scenarios including cross-partition updates where a tuple moves between partitions, foreign data wrapper triggers, and transition table requirements for OLD/NEW table references in triggers. The function retrieves the old tuple from either disk or FDW-supplied data, then delegates to the after-trigger event system for deferred execution. It includes validation to prevent unsupported operations like transition table capture from foreign child tables.

## Parameters / Member Variables
- : Executor state containing execution context and memory management
- : Primary relation information for the root/target table
- : Source partition relation info for cross-partition updates (NULL otherwise)
- : Destination partition relation info for cross-partition updates (NULL otherwise)
- : ItemPointer to the old tuple in source partition (if applicable)
- : Pre-supplied old tuple from FDW (NULL if using tupleid)
- : TupleTableSlot containing the new tuple values after update
- : List of indexes that need rechecking after the update
- : State for capturing OLD/NEW table data for triggers
- : Flag indicating this is a cross-partition update operation

## Dependencies
- Functions called/Symbols referenced:
  - [ExecGetTriggerOldSlot](ExecGetTriggerOldSlot.md)
  - [GetTupleForTrigger](../G/GetTupleForTrigger.md)
  - [ExecForceStoreHeapTuple](ExecForceStoreHeapTuple.md)
  - ExecClearTuple
  - AfterTriggerSaveEvent
  - [ExecGetAllUpdatedCols](ExecGetAllUpdatedCols.md)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
- Called from (representative examples):
  - [ExecUpdateEpilogue](ExecUpdateEpilogue.md)
  - [ExecCrossPartitionUpdateForeignKey](ExecCrossPartitionUpdateForeignKey.md)
  - [ExecSimpleRelationUpdate](ExecSimpleRelationUpdate.md)

## Notes and Other Information
- Does not execute triggers immediately - saves events for deferred execution via AfterTriggerSaveEvent
- Supports cross-partition updates where source and destination partitions differ
- Validates that transition table capture is not attempted on foreign child tables
- Handles cases where either old tuple or new tuple may be NULL during partition key updates
- Uses LockTupleExclusive when retrieving old tuples to ensure consistency
- Clears oldslot when neither tupleid nor fdw_trigtuple is provided