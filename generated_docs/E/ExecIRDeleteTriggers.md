# ExecIRDeleteTriggers

## Location
[src/backend/commands/trigger.c:2859-2905](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2859-L2905)

## Overview
ExecIRDeleteTriggers executes INSTEAD OF ROW DELETE triggers on views, allowing views to handle DELETE operations through custom trigger logic.

## Definition

```c
bool
ExecIRDeleteTriggers(EState *estate, ResultRelInfo *relinfo,
					 HeapTuple trigtuple)
```
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
  - [ExecGetTriggerOldSlot](ExecGetTriggerOldSlot.md)
  - [ExecForceStoreHeapTuple](ExecForceStoreHeapTuple.md)
  - [ExecCallTriggerFunc](ExecCallTriggerFunc.md)
  - [TriggerEnabled](../T/TriggerEnabled.md)
  - GetPerTupleMemoryContext
  - [heap_freetuple](../h/heap_freetuple.md)
- Data types referenced:
  - [TriggerDesc](../T/TriggerDesc.md)
  - [TriggerData](../T/TriggerData.md)
  - [Trigger](../T/Trigger.md)
  - TRIGGER_EVENT_DELETE
  - TRIGGER_EVENT_ROW
  - TRIGGER_EVENT_INSTEAD
  - TRIGGER_TYPE_ROW
  - TRIGGER_TYPE_INSTEAD
  - TRIGGER_TYPE_DELETE
- Macros used:
  - TRIGGER_TYPE_MATCHES
- Called from (representative examples):
  - [ExecDelete](ExecDelete.md)
  - [ExecMergeMatched](ExecMergeMatched.md)

## Notes and Other Information
- Only applies to views with INSTEAD OF DELETE triggers, not regular tables
- Returns true if the delete was processed by triggers, false if any trigger suppressed the operation
- Triggers are executed in definition order until one returns NULL or all complete successfully
- The actual delete logic is entirely implemented by the trigger functions
- Memory management includes proper cleanup of trigger return values
- Located in src/backend/commands/trigger.c:2859-2905
- Part of PostgreSQL's system for making views updatable through trigger logic

## Simplified Source

```c
bool ExecIRDeleteTriggers(EState *estate, ResultRelInfo *relinfo,
                         HeapTuple trigtuple)
{
    TriggerDesc *trigdesc = relinfo->ri_TrigDesc;
    TupleTableSlot *slot = ExecGetTriggerOldSlot(estate, relinfo);
    TriggerData LocTriggerData = {0};

    // Initialize trigger data for DELETE operation
    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_DELETE |
                              TRIGGER_EVENT_ROW |
                              TRIGGER_EVENT_INSTEAD;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;

    ExecForceStoreHeapTuple(trigtuple, slot, false);

    // Execute each INSTEAD OF ROW DELETE trigger
    for (int i = 0; i < trigdesc->numtriggers; i++)
    {
        Trigger *trigger = &trigdesc->triggers[i];

        // Skip non-matching triggers
        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype,
                                 TRIGGER_TYPE_ROW,
                                 TRIGGER_TYPE_INSTEAD,
                                 TRIGGER_TYPE_DELETE))
            continue;
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event,
                           NULL, slot, NULL))
            continue;

        // Set up trigger data and execute trigger function
        LocTriggerData.tg_trigslot = slot;
        LocTriggerData.tg_trigtuple = trigtuple;
        LocTriggerData.tg_trigger = trigger;

        HeapTuple rettuple = ExecCallTriggerFunc(&LocTriggerData, i,
                                               relinfo->ri_TrigFunctions,
                                               relinfo->ri_TrigInstrument,
                                               GetPerTupleMemoryContext(estate));

        if (rettuple == NULL)
            return false; // Delete was suppressed
        if (rettuple != trigtuple)
            heap_freetuple(rettuple);
    }
    return true;
}
```