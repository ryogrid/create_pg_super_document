# ExecIRInsertTriggers

## Location
src/backend/commands/trigger.c: 2562 - 2622

## Overview
Executes INSTEAD OF ROW INSERT triggers for views, allowing triggers to replace the default insert operation with custom logic.

## Definition
```c
bool ExecIRInsertTriggers(EState *estate, ResultRelInfo *relinfo,
                         TupleTableSlot *slot)
```

## Detailed Description
ExecIRInsertTriggers executes INSTEAD OF ROW INSERT triggers, which are primarily used with views to provide custom insert behavior. Unlike AFTER triggers, INSTEAD OF triggers execute immediately and can modify or replace the tuple being inserted. The function iterates through all applicable triggers, calling each one in sequence and allowing each trigger to potentially modify the tuple data.

The function handles memory management carefully, fetching heap tuples only when needed and freeing them appropriately. If any trigger returns NULL, the insert operation is cancelled ("do nothing" semantics). If a trigger returns a modified tuple, the slot is updated with the new data. The function returns false if the insert should be cancelled, true otherwise.

This is a critical component for view insertability in PostgreSQL, enabling complex business logic to be implemented through triggers on views.

## Parameters / Member Variables
- `estate`: Execution state containing transaction and query context information
- `relinfo`: Information about the target relation including trigger descriptors and cached trigger functions
- `slot`: TupleTableSlot containing the tuple data to be inserted, may be modified by triggers

## Dependencies
- Functions called/Symbols referenced:
  - [ExecFetchSlotHeapTuple](ExecFetchSlotHeapTuple.md)
  - [ExecCallTriggerFunc](ExecCallTriggerFunc.md)
  - [ExecForceStoreHeapTuple](ExecForceStoreHeapTuple.md)
  - [TriggerEnabled](../T/TriggerEnabled.md)
  - GetPerTupleMemoryContext
  - [heap_freetuple](../h/heap_freetuple.md)
  - TRIGGER_TYPE_MATCHES
- Constants used:
  - TRIGGER_EVENT_INSERT
  - TRIGGER_EVENT_ROW
  - TRIGGER_EVENT_INSTEAD
  - TRIGGER_TYPE_ROW
  - TRIGGER_TYPE_INSTEAD
  - TRIGGER_TYPE_INSERT
- Data structures used:
  - TriggerDesc
  - TriggerData
  - Trigger
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md)
  - [ExecInsert](ExecInsert.md)

## Notes and Other Information
- Returns false if any trigger cancels the insert operation (returns NULL), true otherwise
- Triggers execute immediately, not deferred like AFTER triggers
- Each trigger can modify the tuple data, and subsequent triggers see the modified data
- Memory management is handled carefully with should_free tracking to avoid double-frees
- INSTEAD OF triggers are commonly used to make views insertable
- The function properly handles the case where triggers return the same tuple vs. a modified tuple
- Trigger functions are cached in relinfo->ri_TrigFunctions for performance