# AfterTriggerExecute

## Location
[src/backend/commands/trigger.c:4355-4629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L4355-L4629)

## Overview
Executes a single after-trigger function by fetching the required tuples and calling the trigger with proper context setup and memory management.

## Definition
```c
static void AfterTriggerExecute(EState *estate, AfterTriggerEvent event, ResultRelInfo *relInfo, ResultRelInfo *src_relInfo, ResultRelInfo *dst_relInfo, TriggerDesc *trigdesc, FmgrInfo *finfo, Instrumentation *instr, MemoryContext per_tuple_context, TupleTableSlot *trig_tuple_slot1, TupleTableSlot *trig_tuple_slot2)
```

## Detailed Description
This function is the core execution engine for individual after-trigger events. It handles the complex process of fetching the appropriate tuples (old and/or new) from the heap, setting up the trigger context, and executing the trigger function. The function supports multiple scenarios including regular heap tables, foreign tables (with FDW tuple fetching), and cross-partition updates in partitioned tables. It performs tuple conversion when necessary for partitioned tables, manages transition tables for triggers that need them, and handles proper memory cleanup. The function also supports EXPLAIN ANALYZE instrumentation and implements caching optimizations for repeated trigger executions on the same relation.

## Parameters / Member Variables
- `estate`: Executor state containing transaction and relation information
- `event`: The specific after-trigger event being processed
- `relInfo`: ResultRelInfo for the main target relation
- `src_relInfo`: ResultRelInfo for source partition (cross-partition updates)
- `dst_relInfo`: ResultRelInfo for destination partition (cross-partition updates)
- `trigdesc`: Working copy of the relation's trigger descriptor
- `finfo`: Array of fmgr lookup cache entries for trigger functions
- `instr`: Array of EXPLAIN ANALYZE instrumentation nodes (can be NULL)
- `per_tuple_context`: Memory context for trigger function execution
- `trig_tuple_slot1`: Scratch slot for tg_trigtuple (foreign tables)
- `trig_tuple_slot2`: Scratch slot for tg_newtuple (foreign tables)

## Dependencies
- Functions called/Symbols referenced:
  - GetTriggerSharedData
  - [InstrStartNode](../I/InstrStartNode.md)
  - [GetCurrentFDWTuplestore](../G/GetCurrentFDWTuplestore.md)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md)
  - [ExecFetchSlotHeapTuple](../E/ExecFetchSlotHeapTuple.md)
  - [ExecGetTriggerOldSlot](../E/ExecGetTriggerOldSlot.md)
  - [ExecGetTriggerNewSlot](../E/ExecGetTriggerNewSlot.md)
  - [table_tuple_fetch_row_version](../t/table_tuple_fetch_row_version.md)
  - [ExecGetChildToRootMap](../E/ExecGetChildToRootMap.md)
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md)
  - [ExecCopySlot](../E/ExecCopySlot.md)
  - [ExecCallTriggerFunc](../E/ExecCallTriggerFunc.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [InstrStopNode](../I/InstrStopNode.md)
- Called from (representative examples):
  - [afterTriggerInvokeEvents](../a/afterTriggerInvokeEvents.md)

## Notes and Other Information
- Handles graceful scenarios where triggers may have been dropped since event queuing
- Supports both row-level triggers and transition table processing
- Implements efficient memory management with proper cleanup of temporary tuples
- Critical for cross-partition update support in partitioned tables
- The function is highly optimized for performance with caching mechanisms
- Proper handling of foreign table trigger execution through FDW interfaces

## Simplified Source

```c
static void
AfterTriggerExecute(EState *estate, AfterTriggerEvent event, ResultRelInfo *relInfo,
                    ResultRelInfo *src_relInfo, ResultRelInfo *dst_relInfo,
                    TriggerDesc *trigdesc, FmgrInfo *finfo, Instrumentation *instr,
                    MemoryContext per_tuple_context, TupleTableSlot *trig_tuple_slot1,
                    TupleTableSlot *trig_tuple_slot2)
{
    // Get trigger info from event
    AfterTriggerShared evtshared = GetTriggerSharedData(event);
    Oid tgoid = evtshared->ats_tgoid;
    TriggerData LocTriggerData = {0};
    int tgindx;

    // Find trigger in descriptor (may have been dropped)
    if (!trigdesc) return;
    for (tgindx = 0; tgindx < trigdesc->numtriggers; tgindx++) {
        if (trigdesc->triggers[tgindx].tgoid == tgoid) {
            LocTriggerData.tg_trigger = &(trigdesc->triggers[tgindx]);
            break;
        }
    }
    if (!LocTriggerData.tg_trigger) return;

    // Start timing if doing EXPLAIN ANALYZE
    if (instr) InstrStartNode(instr + tgindx);

    // Fetch required tuples based on event type
    switch (event->ate_flags & AFTER_TRIGGER_TUP_BITS) {
        case AFTER_TRIGGER_FDW_FETCH:
            // Fetch from foreign data wrapper tuplestore
            // ... fetch old/new tuples for FDW
            break;

        default:
            // Fetch from heap using CTIDs
            if (ItemPointerIsValid(&(event->ate_ctid1))) {
                // Fetch old tuple, handle partition mapping if needed
                // ... fetch and convert tuple
            }
            if (ItemPointerIsValid(&(event->ate_ctid2))) {
                // Fetch new tuple for UPDATE
                // ... fetch and convert tuple
            }
            break;
    }

    // Setup transition tables if trigger uses them
    if (evtshared->ats_table) {
        if (LocTriggerData.tg_trigger->tgoldtable) {
            // Setup old table reference
            evtshared->ats_table->closed = true;
        }
        if (LocTriggerData.tg_trigger->tgnewtable) {
            // Setup new table reference
            evtshared->ats_table->closed = true;
        }
    }

    // Setup trigger context
    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = evtshared->ats_event & (TRIGGER_EVENT_OPMASK | TRIGGER_EVENT_ROW);
    LocTriggerData.tg_relation = relInfo->ri_RelationDesc;

    // Reset per-tuple memory context
    MemoryContextReset(per_tuple_context);

    // Execute the trigger function
    HeapTuple rettuple = ExecCallTriggerFunc(&LocTriggerData, tgindx, finfo,
                                             NULL, per_tuple_context);

    // Clean up returned tuple if needed
    if (rettuple && rettuple != LocTriggerData.tg_trigtuple &&
        rettuple != LocTriggerData.tg_newtuple) {
        heap_freetuple(rettuple);
    }

    // Release resources and clear slots
    // ... cleanup code

    // Stop timing if doing EXPLAIN ANALYZE
    if (instr) InstrStopNode(instr + tgindx, 1);
}
```