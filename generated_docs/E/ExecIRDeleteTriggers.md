# ExecIRDeleteTriggers

## Location
src/backend/commands/trigger.c: 2859 - 2905

## Overview
ExecIRDeleteTriggers executes INSTEAD OF ROW DELETE triggers on views, allowing views to handle DELETE operations through custom trigger logic.

## Definition


## Detailed Description
This function executes INSTEAD OF ROW DELETE triggers, which are special triggers that can only be defined on views (not tables). When a DELETE operation is performed on a view that has INSTEAD OF DELETE triggers, these triggers completely replace the default delete behavior.

The function iterates through all triggers defined on the relation, filtering for INSTEAD OF ROW DELETE triggers. For each matching and enabled trigger, it calls the trigger function with the tuple to be deleted. If any trigger returns NULL, the delete operation is considered suppressed. Otherwise, the function returns true to indicate the delete was processed by the triggers.

INSTEAD OF triggers are commonly used to make views updatable by defining custom logic for how DELETE operations should be handled on the underlying base tables.

## Parameters / Member Variables
- : Executor state containing execution context and memory management information
- : ResultRelInfo containing relation metadata and trigger information for the view
- : HeapTuple representing the row being deleted from the view

## Dependencies
- Functions called/Symbols referenced:
  - ExecGetTriggerOldSlot
  - ExecForceStoreHeapTuple
  - ExecCallTriggerFunc
  - TriggerEnabled
  - GetPerTupleMemoryContext
  - heap_freetuple
- Data types referenced:
  - TriggerDesc
  - TriggerData
  - Trigger
  - TRIGGER_EVENT_DELETE
  - TRIGGER_EVENT_ROW
  - TRIGGER_EVENT_INSTEAD
  - TRIGGER_TYPE_ROW
  - TRIGGER_TYPE_INSTEAD
  - TRIGGER_TYPE_DELETE
- Macros used:
  - TRIGGER_TYPE_MATCHES
- Called from (representative examples):
  - ExecDelete
  - ExecMergeMatched

## Notes and Other Information
- Only applies to views with INSTEAD OF DELETE triggers, not regular tables
- Returns true if the delete was processed by triggers, false if any trigger suppressed the operation
- Triggers are executed in definition order until one returns NULL or all complete successfully
- The actual delete logic is entirely implemented by the trigger functions
- Memory management includes proper cleanup of trigger return values
- Located in src/backend/commands/trigger.c:2859-2905
- Part of PostgreSQL's system for making views updatable through trigger logic