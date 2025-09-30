# ExecIRInsertTriggers

## Location
[src/backend/commands/trigger.c:2562-2622](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2562-L2622)

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
  - [TriggerDesc](../T/TriggerDesc.md)
  - [TriggerData](../T/TriggerData.md)
  - [Trigger](../T/Trigger.md)
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
- [Trigger](../T/Trigger.md) functions are cached in relinfo->ri_TrigFunctions for performance

## Simplified Source

```c
bool
ExecIRInsertTriggers(EState *estate, ResultRelInfo *relinfo, TupleTableSlot *slot)
{
    TriggerDesc *trigdesc = relinfo->ri_TrigDesc;
    HeapTuple newtuple = NULL;
    bool should_free;
    TriggerData LocTriggerData = {0};

    // Set up trigger context for INSTEAD OF ROW INSERT
    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW | TRIGGER_EVENT_INSTEAD;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;

    // Execute each applicable INSTEAD OF ROW INSERT trigger
    for (int i = 0; i < trigdesc->numtriggers; i++)
    {
        Trigger *trigger = &trigdesc->triggers[i];

        // Skip if not an INSTEAD OF ROW INSERT trigger or if disabled
        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_ROW,
                                 TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_INSERT))
            continue;
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event,
                           NULL, NULL, slot))
            continue;

        // Convert slot to HeapTuple if needed
        if (!newtuple)
            newtuple = ExecFetchSlotHeapTuple(slot, true, &should_free);

        // Execute trigger function
        HeapTuple oldtuple = newtuple;
        LocTriggerData.tg_trigslot = slot;
        LocTriggerData.tg_trigtuple = oldtuple;
        LocTriggerData.tg_trigger = trigger;

        newtuple = ExecCallTriggerFunc(&LocTriggerData, i,
                                     relinfo->ri_TrigFunctions,
                                     relinfo->ri_TrigInstrument,
                                     GetPerTupleMemoryContext(estate));

        // Handle trigger result
        if (newtuple == NULL)
        {
            // Trigger wants to cancel this insert
            if (should_free)
                heap_freetuple(oldtuple);
            return false;
        }
        else if (newtuple != oldtuple)
        {
            // Trigger modified the tuple
            ExecForceStoreHeapTuple(newtuple, slot, false);

            if (should_free)
                heap_freetuple(oldtuple);
            newtuple = NULL; // Signal for re-fetch if needed
        }
    }

    return true;
}
```