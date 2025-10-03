# ExecIRUpdateTriggers

## Location
[src/backend/commands/trigger.c:3241-3306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L3241-L3306)

## Overview
Executes INSTEAD OF ROW UPDATE triggers for views, allowing user-defined logic to replace the default update operation with custom handling.

## Definition

```c
bool
ExecIRUpdateTriggers(EState *estate, ResultRelInfo *relinfo,
					 HeapTuple trigtuple, TupleTableSlot *newslot)
```
## Detailed Description
This function executes INSTEAD OF ROW UPDATE triggers, which are primarily used with views to provide custom update logic. Unlike BEFORE/AFTER triggers, INSTEAD OF triggers completely replace the normal update operation. The function iterates through all applicable INSTEAD OF UPDATE triggers, calling each one in sequence. If any trigger returns NULL, the entire operation is canceled. Triggers can modify the new tuple values, and the function ensures proper memory management throughout the process.

## Parameters / Member Variables
- : Executor state containing execution context and memory management
- : Relation information including trigger descriptors and view metadata
- : The original tuple being updated (serves as the OLD tuple)
- : TupleTableSlot containing the new tuple values after update

## Dependencies
- Functions called/Symbols referenced:
  - [ExecGetTriggerOldSlot](ExecGetTriggerOldSlot.md)
  - [ExecForceStoreHeapTuple](ExecForceStoreHeapTuple.md)
  - [TriggerEnabled](../T/TriggerEnabled.md)
  - [ExecFetchSlotHeapTuple](ExecFetchSlotHeapTuple.md)
  - [ExecCallTriggerFunc](ExecCallTriggerFunc.md)
  - GetPerTupleMemoryContext
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ExecUpdate](ExecUpdate.md)
  - [ExecMergeMatched](ExecMergeMatched.md)

## Notes and Other Information
- Returns false if any trigger cancels the operation by returning NULL
- Primarily used for views since regular tables use BEFORE/AFTER triggers
- Triggers execute immediately and synchronously, unlike AFTER triggers
- Updates newslot in-place when triggers modify the new tuple values
- Manages memory carefully by tracking which HeapTuples need to be freed
- Does not use updated column information (passes NULL to TriggerEnabled)
- Each trigger can potentially modify the result of previous triggers in the chain

## Simplified Source

```c
bool
ExecIRUpdateTriggers(EState *estate, ResultRelInfo *relinfo,
                     HeapTuple trigtuple, TupleTableSlot *newslot)
{
    TriggerDesc *trigdesc = relinfo->ri_TrigDesc;
    TupleTableSlot *oldslot = ExecGetTriggerOldSlot(estate, relinfo);
    HeapTuple newtuple = NULL;
    bool should_free;
    TriggerData LocTriggerData = {0};

    // Set up trigger event data
    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_UPDATE |
                             TRIGGER_EVENT_ROW |
                             TRIGGER_EVENT_INSTEAD;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;

    // Store the old tuple in the trigger slot
    ExecForceStoreHeapTuple(trigtuple, oldslot, false);

    // Execute each applicable INSTEAD OF UPDATE trigger
    for (int i = 0; i < trigdesc->numtriggers; i++) {
        Trigger *trigger = &trigdesc->triggers[i];

        // Skip triggers that don't match our criteria
        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype,
                                TRIGGER_TYPE_ROW,
                                TRIGGER_TYPE_INSTEAD,
                                TRIGGER_TYPE_UPDATE))
            continue;

        // Skip disabled triggers
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event,
                          NULL, oldslot, newslot))
            continue;

        // Convert new slot to HeapTuple if not already done
        if (!newtuple)
            newtuple = ExecFetchSlotHeapTuple(newslot, true, &should_free);

        // Set up trigger data
        LocTriggerData.tg_trigslot = oldslot;
        LocTriggerData.tg_trigtuple = trigtuple;
        LocTriggerData.tg_newslot = newslot;
        LocTriggerData.tg_newtuple = newtuple;
        LocTriggerData.tg_trigger = trigger;

        // Execute the trigger function
        HeapTuple oldtuple = newtuple;
        newtuple = ExecCallTriggerFunc(&LocTriggerData, i,
                                     relinfo->ri_TrigFunctions,
                                     relinfo->ri_TrigInstrument,
                                     GetPerTupleMemoryContext(estate));

        if (newtuple == NULL) {
            return false; // Trigger canceled the operation
        }

        // If trigger modified the tuple, update the slot
        if (newtuple != oldtuple) {
            ExecForceStoreHeapTuple(newtuple, newslot, false);

            // Free old tuple if we allocated it
            if (should_free)
                heap_freetuple(oldtuple);

            // Signal that tuple should be re-fetched next time
            newtuple = NULL;
        }
    }

    return true; // All triggers executed successfully
}
```