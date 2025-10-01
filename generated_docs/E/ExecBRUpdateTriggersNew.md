# ExecBRUpdateTriggersNew

## Location
[src/backend/commands/trigger.c:2982-3146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2982-L3146)

## Overview
Executes BEFORE ROW UPDATE triggers for a new tuple, handling concurrent updates through EPQ (EvalPlanQual) rechecking and managing trigger execution workflow with proper memory management.

## Definition

```c
bool
ExecBRUpdateTriggersNew(EState *estate, EPQState *epqstate,
						ResultRelInfo *relinfo,
						ItemPointer tupleid,
						HeapTuple fdw_trigtuple,
						TupleTableSlot *newslot,
						TM_Result *tmresult,
						TM_FailureData *tmfd,
						bool is_merge_update)
```
## Detailed Description
This function executes BEFORE ROW UPDATE triggers for update operations, providing robust handling of concurrent updates through EPQ (EvalPlanQual) mechanisms. It retrieves the old tuple either from disk (using tupleid) or from FDW-supplied data (fdw_trigtuple), prepares trigger data structures, and iterates through all applicable BEFORE UPDATE triggers. The function handles memory management carefully, materializing slots when necessary to prevent dangling references, and supports both regular UPDATE and MERGE UPDATE operations with different EPQ behaviors.

## Parameters / Member Variables
- : Executor state containing execution context and memory management
- : EPQ state for handling concurrent tuple modifications
- : Relation information including trigger descriptors and metadata
- : ItemPointer to the target tuple on disk (NULL if using fdw_trigtuple)
- : Pre-supplied tuple from FDW (NULL if using tupleid)
- : TupleTableSlot containing the new tuple values after update
- : Output parameter for tuple manager operation result
- : Output parameter for tuple manager failure data
- : Flag indicating if this is a MERGE UPDATE (affects EPQ behavior)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecGetTriggerOldSlot](ExecGetTriggerOldSlot.md)
  - [ExecUpdateLockMode](ExecUpdateLockMode.md)
  - [GetTupleForTrigger](../G/GetTupleForTrigger.md)
  - [ExecGetUpdateNewTuple](ExecGetUpdateNewTuple.md)
  - [ExecFetchSlotHeapTuple](ExecFetchSlotHeapTuple.md)
  - [ExecGetAllUpdatedCols](ExecGetAllUpdatedCols.md)
  - [TriggerEnabled](../T/TriggerEnabled.md)
  - [ExecCallTriggerFunc](ExecCallTriggerFunc.md)
  - [ExecMaterializeSlot](ExecMaterializeSlot.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ExecBRUpdateTriggers](ExecBRUpdateTriggers.md)
  - [ExecUpdatePrologue](ExecUpdatePrologue.md)

## Notes and Other Information
- Returns false if any trigger cancels the operation or if tuple retrieval fails
- Handles EPQ rechecking for concurrent updates, but skips it for MERGE UPDATE operations
- Properly manages memory by tracking which HeapTuples need to be freed
- Materializes slots when necessary to prevent references to unpinned buffers
- Supports both disk-based tuples (via tupleid) and FDW-supplied tuples (via fdw_trigtuple)
- Updates newslot in-place when EPQ processing provides a newer tuple version

## Simplified Source

```c
bool ExecBRUpdateTriggersNew(EState *estate, EPQState *epqstate,
                            ResultRelInfo *relinfo, ItemPointer tupleid,
                            HeapTuple fdw_trigtuple, TupleTableSlot *newslot,
                            TM_Result *tmresult, TM_FailureData *tmfd,
                            bool is_merge_update) {
    TriggerDesc *trigdesc = relinfo->ri_TrigDesc;
    TupleTableSlot *oldslot = ExecGetTriggerOldSlot(estate, relinfo);
    HeapTuple newtuple = NULL, trigtuple;
    TriggerData LocTriggerData = {0};
    BitmapSet *updatedCols;
    LockTupleMode lockmode = ExecUpdateLockMode(estate, relinfo);

    // Get the old tuple - either from disk or FDW-supplied
    if (fdw_trigtuple == NULL) {
        // Get tuple from disk with EPQ handling for concurrent updates
        if (!GetTupleForTrigger(estate, epqstate, relinfo, tupleid,
                               lockmode, oldslot, !is_merge_update,
                               &epqslot_candidate, tmresult, tmfd))
            return false;  // Cancel update

        // Handle EPQ recheck if tuple was concurrently modified
        if (epqslot_candidate != NULL) {
            TupleTableSlot *epqslot_clean = ExecGetUpdateNewTuple(relinfo,
                                                                 epqslot_candidate, oldslot);
            if (newslot != epqslot_clean)
                ExecCopySlot(newslot, epqslot_clean);
            ExecMaterializeSlot(newslot);  // Prevent dangling buffer references
        }
        trigtuple = ExecFetchSlotHeapTuple(oldslot, true, &should_free_trig);
    } else {
        // Use FDW-supplied tuple
        ExecForceStoreHeapTuple(fdw_trigtuple, oldslot, false);
        trigtuple = fdw_trigtuple;
    }

    // Setup trigger data structure
    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_UPDATE | TRIGGER_EVENT_ROW | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;
    updatedCols = ExecGetAllUpdatedCols(relinfo, estate);
    LocTriggerData.tg_updatedcols = updatedCols;

    // Execute all applicable BEFORE UPDATE triggers
    for (int i = 0; i < trigdesc->numtriggers; i++) {
        Trigger *trigger = &trigdesc->triggers[i];

        // Check if this trigger applies to BEFORE UPDATE ROW events
        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_ROW,
                                 TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE))
            continue;
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event,
                           updatedCols, oldslot, newslot))
            continue;

        // Prepare trigger execution data
        if (!newtuple)
            newtuple = ExecFetchSlotHeapTuple(newslot, true, &should_free_new);

        LocTriggerData.tg_trigslot = oldslot;
        LocTriggerData.tg_trigtuple = trigtuple;
        LocTriggerData.tg_newtuple = newtuple;
        LocTriggerData.tg_newslot = newslot;
        LocTriggerData.tg_trigger = trigger;

        // Execute the trigger function
        HeapTuple oldtuple = newtuple;
        newtuple = ExecCallTriggerFunc(&LocTriggerData, i,
                                      relinfo->ri_TrigFunctions,
                                      relinfo->ri_TrigInstrument,
                                      GetPerTupleMemoryContext(estate));

        if (newtuple == NULL) {
            // Trigger canceled the operation
            return false;
        } else if (newtuple != oldtuple) {
            // Trigger modified the tuple
            ExecForceStoreHeapTuple(newtuple, newslot, false);
            if (should_free_trig && newtuple == trigtuple)
                ExecMaterializeSlot(newslot);
            newtuple = NULL;  // Signal re-fetch needed
        }
    }

    return true;  // All triggers succeeded
}
```