# ExecIRUpdateTriggers

## Location
src/backend/commands/trigger.c: 3241 - 3306

## Overview
Executes INSTEAD OF ROW UPDATE triggers for views, allowing user-defined logic to replace the default update operation with custom handling.

## Definition


## Detailed Description
This function executes INSTEAD OF ROW UPDATE triggers, which are primarily used with views to provide custom update logic. Unlike BEFORE/AFTER triggers, INSTEAD OF triggers completely replace the normal update operation. The function iterates through all applicable INSTEAD OF UPDATE triggers, calling each one in sequence. If any trigger returns NULL, the entire operation is canceled. Triggers can modify the new tuple values, and the function ensures proper memory management throughout the process.

## Parameters / Member Variables
- : Executor state containing execution context and memory management
- : Relation information including trigger descriptors and view metadata
- : The original tuple being updated (serves as the OLD tuple)
- : TupleTableSlot containing the new tuple values after update

## Dependencies
- Functions called/Symbols referenced:
  - ExecGetTriggerOldSlot
  - ExecForceStoreHeapTuple
  - TriggerEnabled
  - ExecFetchSlotHeapTuple
  - ExecCallTriggerFunc
  - GetPerTupleMemoryContext
  - heap_freetuple
- Called from (representative examples):
  - ExecUpdate
  - ExecMergeMatched

## Notes and Other Information
- Returns false if any trigger cancels the operation by returning NULL
- Primarily used for views since regular tables use BEFORE/AFTER triggers
- Triggers execute immediately and synchronously, unlike AFTER triggers
- Updates newslot in-place when triggers modify the new tuple values
- Manages memory carefully by tracking which HeapTuples need to be freed
- Does not use updated column information (passes NULL to TriggerEnabled)
- Each trigger can potentially modify the result of previous triggers in the chain