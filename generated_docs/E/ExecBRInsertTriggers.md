# ExecBRInsertTriggers

## Location
[src/backend/commands/trigger.c:2460-2535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2460-L2535)

## Overview
Executes BEFORE ROW INSERT triggers for each tuple being inserted, allowing triggers to modify tuple data or skip the insert operation entirely while ensuring partition constraints are maintained.

## Definition
```c
bool ExecBRInsertTriggers(EState *estate, ResultRelInfo *relinfo,
                          TupleTableSlot *slot)
```

## Detailed Description
This function manages the execution of BEFORE ROW INSERT triggers, which fire once for each tuple being inserted and can modify the tuple data or prevent the insertion. It implements several critical features:

1. **Tuple Modification**: Triggers can return a modified tuple, which replaces the original in the slot
2. **Insert Prevention**: Triggers can return NULL to skip inserting the current tuple
3. **Partition Validation**: For partitioned tables, ensures modified tuples still belong to the correct partition
4. **Memory Management**: Properly handles HeapTuple lifecycle and memory cleanup
5. **Performance Optimization**: Lazy tuple materialization - only converts slot to HeapTuple when needed

The function iterates through all applicable triggers, calling each one with the current tuple data. If a trigger modifies the tuple, the changes are stored back in the slot. If a trigger is defined on a partition (tgisclone), additional validation ensures the modified tuple still fits the partition constraints.

## Parameters / Member Variables
- `estate`: Executor state containing execution context and memory management information
- `relinfo`: Result relation info containing trigger descriptors, function cache, and relation metadata
- `slot`: TupleTableSlot containing the tuple being inserted, which may be modified by triggers

## Return Value
- `true`: Insert operation should proceed (possibly with modified tuple data)
- `false`: Insert operation should be skipped (trigger returned NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecFetchSlotHeapTuple](ExecFetchSlotHeapTuple.md) (converts slot to HeapTuple for trigger processing)
  - TRIGGER_TYPE_MATCHES (trigger type filtering macro)
  - [TriggerEnabled](../T/TriggerEnabled.md) (trigger enable state and condition checking)
  - [ExecCallTriggerFunc](ExecCallTriggerFunc.md) (actual trigger execution)
  - [ExecForceStoreHeapTuple](ExecForceStoreHeapTuple.md) (stores modified tuple back to slot)
  - [ExecPartitionCheck](ExecPartitionCheck.md) (validates partition constraints)
  - [heap_freetuple](../h/heap_freetuple.md) (memory cleanup)
  - GetPerTupleMemoryContext (memory context management)
- Data structures used:
  - [TriggerData](../T/TriggerData.md) (trigger execution context)
  - [TriggerDesc](../T/TriggerDesc.md) (trigger descriptor from relinfo)
  - [Trigger](../T/Trigger.md) (individual trigger structure)
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md) (during COPY FROM operations)
  - [ExecInsert](ExecInsert.md) (from nodeModifyTable executor)
  - [ExecSimpleRelationInsert](ExecSimpleRelationInsert.md) (logical replication)

## Notes and Other Information
- BEFORE ROW triggers fire once per tuple and can inspect/modify individual row data
- Triggers returning NULL effectively act as row-level filters, preventing specific inserts
- The partition check prevents data inconsistency when triggers modify partition keys
- Memory management is carefully handled to avoid leaks during tuple modifications
- [Trigger](../T/Trigger.md) execution order follows the creation order of triggers on the table
- The function supports both regular tables and partitioned table hierarchies
- Critical for maintaining data integrity while allowing flexible business logic implementation
- Used in high-throughput operations like COPY FROM where performance optimization is essential

## Simplified Source

```c
bool
ExecBRInsertTriggers(EState *estate, ResultRelInfo *relinfo, TupleTableSlot *slot)
{
    TriggerDesc *trigdesc = relinfo->ri_TrigDesc;
    HeapTuple newtuple = NULL;
    bool should_free;
    TriggerData LocTriggerData = {0};

    // Set up trigger context
    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;

    // Execute each applicable BEFORE ROW INSERT trigger
    for (int i = 0; i < trigdesc->numtriggers; i++)
    {
        Trigger *trigger = &trigdesc->triggers[i];

        // Skip if not a BEFORE ROW INSERT trigger or if disabled
        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_ROW,
                                 TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT))
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
            // Trigger wants to skip this insert
            if (should_free)
                heap_freetuple(oldtuple);
            return false;
        }
        else if (newtuple != oldtuple)
        {
            // Trigger modified the tuple
            ExecForceStoreHeapTuple(newtuple, slot, false);

            // Validate partition constraints for modified tuple
            if (trigger->tgisclone && !ExecPartitionCheck(relinfo, slot, estate, false))
            {
                ereport(ERROR, "moving row to another partition during trigger not supported");
            }

            if (should_free)
                heap_freetuple(oldtuple);
            newtuple = NULL; // Signal for re-fetch if needed
        }
    }

    return true;
}
```