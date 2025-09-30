# AfterTriggerSaveEvent

## Location
[src/backend/commands/trigger.c:6148-6544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L6148-L6544)

## Overview
Queues after-trigger events for execution at transaction commit, handling both row-level and statement-level triggers while managing transition table capture and cross-partition update scenarios.

## Definition

```c
structure and add it to the current query's queue.
		 * Note we set ats_table to NULL whenever this trigger doesn't use
		 * transition tables, to improve sharability of the shared event data.
		 */
		new_shared.ats_event =
			(event & TRIGGER_EVENT_OPMASK) |
			(row_trigger ? TRIGGER_EVENT_ROW : 0) |
			(trigger->tgdeferrable ? AFTER_TRIGGER_DEFERRABLE : 0) |
			(trigger->tginitdeferred ? AFTER_TRIGGER_INITDEFERRED : 0);
```
## Detailed Description
This function is the central mechanism for queuing after-trigger events in PostgreSQL's deferred trigger system. It is called by the ExecA[RS]...Triggers() family of functions whenever triggers need to be queued for later execution, even when triggers are disabled (the function determines which triggers actually need queuing).

The function handles several complex scenarios:

**Transition Table Management**: Builds transition tuplestores immediately rather than during event execution, allowing AFTER ROW triggers to select from transition tables. This is critical for statement-level triggers that use OLD/NEW table references.

**Cross-Partition Updates**: Contains special logic for partitioned tables undergoing cross-partition updates with foreign key constraints. When a row moves between partitions (implemented as DELETE + INSERT), the function ensures proper foreign key trigger behavior by queuing UPDATE events on the root partitioned table instead of separate DELETE/INSERT events.

**Event Validation and CTID Tracking**: Validates event codes and captures tuple CTIDs (both old and new for updates) that will be needed during trigger execution. For partitioned table updates, it also stores source and destination partition OIDs.

**Optimization Logic**: Includes sophisticated optimization for foreign key triggers, skipping unnecessary events when constraints can be verified not to be violated. Also handles deferred unique constraint triggers by only queuing events when violations are possible.

**Statement-Level Trigger Management**: For statement-level triggers, cancels any previously queued events for the same statement to ensure triggers fire only once per statement and after all row-level triggers.

## Parameters / Member Variables
- : Executor state containing execution context and tuple slots
- : Information about the target relation for the trigger event
- : Source partition info for cross-partition updates (NULL otherwise)
- : Destination partition info for cross-partition updates (NULL otherwise)
- : Trigger event type (INSERT, UPDATE, DELETE, TRUNCATE)
- : True for row-level triggers, false for statement-level triggers
- : Tuple slot containing the old tuple (for UPDATE/DELETE)
- : Tuple slot containing the new tuple (for INSERT/UPDATE)
- : List of indexes requiring uniqueness rechecking
- : Bitmapset of columns modified by the operation
- : State for capturing transition table data
- : True when handling cross-partition update scenarios

## Dependencies
- Functions called/Symbols referenced:
  - [AfterTriggerEnlargeQueryState](AfterTriggerEnlargeQueryState.md) (ensures adequate query depth storage)
  - [GetAfterTriggersTransitionTable](../G/GetAfterTriggersTransitionTable.md) (manages transition table creation)
  - [TransitionTableAddTuple](../T/TransitionTableAddTuple.md) (adds tuples to transition tables)
  - [cancel_prior_stmt_triggers](../c/cancel_prior_stmt_triggers.md) (cancels previous statement-level triggers)
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md) (converts tuples between partition formats)
  - [ExecGetTriggerOldSlot](../E/ExecGetTriggerOldSlot.md)/ExecGetTriggerNewSlot (gets trigger tuple slots)
  - [TriggerEnabled](../T/TriggerEnabled.md) (checks if trigger should fire)
  - [RI_FKey_trigger_type](../R/RI_FKey_trigger_type.md) (identifies foreign key trigger types)
  - [afterTriggerAddEvent](../a/afterTriggerAddEvent.md) (adds event to trigger queue)
- Called from (representative examples):
  - [ExecASInsertTriggers](../E/ExecASInsertTriggers.md) (for INSERT statement-level triggers)
  - [ExecARInsertTriggers](../E/ExecARInsertTriggers.md) (for INSERT row-level triggers)
  - [ExecASDeleteTriggers](../E/ExecASDeleteTriggers.md) (for DELETE statement-level triggers)
  - [ExecARDeleteTriggers](../E/ExecARDeleteTriggers.md) (for DELETE row-level triggers)
  - [ExecASUpdateTriggers](../E/ExecASUpdateTriggers.md) (for UPDATE statement-level triggers)
  - [ExecARUpdateTriggers](../E/ExecARUpdateTriggers.md) (for UPDATE row-level triggers)
  - [ExecASTruncateTriggers](../E/ExecASTruncateTriggers.md) (for TRUNCATE triggers)

## Notes and Other Information
- The function is called even when no triggers exist for an event if transition tables need to be built for statement-level triggers
- Contains special handling for foreign tables using FDW-specific tuple storage mechanisms
- Implements complex optimization logic for foreign key constraints to avoid unnecessary trigger executions
- The cross-partition update logic ensures that foreign key constraints work correctly when rows move between partitions
- Statement-level triggers are designed to fire exactly once per statement, after all row-level triggers have been processed
- Transition tables are built immediately to support AFTER ROW triggers that need to query transition data

## Simplified Source

```c
static void
AfterTriggerSaveEvent(EState *estate, ResultRelInfo *relinfo,
                     ResultRelInfo *src_partinfo, ResultRelInfo *dst_partinfo,
                     int event, bool row_trigger,
                     TupleTableSlot *oldslot, TupleTableSlot *newslot,
                     List *recheckIndexes, Bitmapset *modifiedCols,
                     TransitionCaptureState *transition_capture,
                     bool is_crosspart_update)
{
    Relation rel = relinfo->ri_RelationDesc;
    TriggerDesc *trigdesc = relinfo->ri_TrigDesc;
    AfterTriggerEventData new_event;
    AfterTriggerSharedData new_shared;

    // Validate query depth
    if (afterTriggers.query_depth < 0)
        elog(ERROR, "AfterTriggerSaveEvent() called outside of query");

    // Ensure adequate storage for current query depth
    if (afterTriggers.query_depth >= afterTriggers.maxquerydepth)
        AfterTriggerEnlargeQueryState();

    // Capture transition tuples if needed
    if (row_trigger && transition_capture != NULL) {
        if (!TupIsNull(oldslot)) {
            Tuplestorestate *old_tuplestore = GetAfterTriggersTransitionTable(event,
                                                                             oldslot, NULL,
                                                                             transition_capture);
            TransitionTableAddTuple(estate, transition_capture, relinfo,
                                  oldslot, NULL, old_tuplestore);
        }

        if (!TupIsNull(newslot)) {
            Tuplestorestate *new_tuplestore = GetAfterTriggersTransitionTable(event,
                                                                             NULL, newslot,
                                                                             transition_capture);
            TransitionTableAddTuple(estate, transition_capture, relinfo,
                                  newslot, transition_capture->tcs_original_insert_tuple,
                                  new_tuplestore);
        }

        // Return early if only transition tables needed
        if (trigdesc == NULL || !trigdesc->trig_insert_after_row)
            return;
    }

    // Set up event data based on trigger type
    switch (event) {
        case TRIGGER_EVENT_INSERT:
            if (row_trigger) {
                ItemPointerCopy(&(newslot->tts_tid), &(new_event.ate_ctid1));
                ItemPointerSetInvalid(&(new_event.ate_ctid2));
            } else {
                cancel_prior_stmt_triggers(RelationGetRelid(rel), CMD_INSERT, event);
            }
            break;

        case TRIGGER_EVENT_DELETE:
            if (row_trigger) {
                ItemPointerCopy(&(oldslot->tts_tid), &(new_event.ate_ctid1));
                ItemPointerSetInvalid(&(new_event.ate_ctid2));
            } else {
                cancel_prior_stmt_triggers(RelationGetRelid(rel), CMD_DELETE, event);
            }
            break;

        case TRIGGER_EVENT_UPDATE:
            if (row_trigger) {
                ItemPointerCopy(&(oldslot->tts_tid), &(new_event.ate_ctid1));
                ItemPointerCopy(&(newslot->tts_tid), &(new_event.ate_ctid2));
                // Handle cross-partition updates
                if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
                    new_event.ate_src_part = RelationGetRelid(src_partinfo->ri_RelationDesc);
                    new_event.ate_dst_part = RelationGetRelid(dst_partinfo->ri_RelationDesc);
                }
            } else {
                cancel_prior_stmt_triggers(RelationGetRelid(rel), CMD_UPDATE, event);
            }
            break;
    }

    // Set event flags
    if (row_trigger && event == TRIGGER_EVENT_UPDATE) {
        new_event.ate_flags = (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) ?
                             AFTER_TRIGGER_CP_UPDATE : AFTER_TRIGGER_2CTID;
    } else {
        new_event.ate_flags = AFTER_TRIGGER_1CTID;
    }

    // Process each trigger
    for (int i = 0; i < trigdesc->numtriggers; i++) {
        Trigger *trigger = &trigdesc->triggers[i];

        // Check if trigger matches event type and is enabled
        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype,
                                 row_trigger ? TRIGGER_TYPE_ROW : TRIGGER_TYPE_STATEMENT,
                                 TRIGGER_TYPE_AFTER, event) ||
            !TriggerEnabled(estate, relinfo, trigger, event, modifiedCols, oldslot, newslot))
            continue;

        // Skip foreign key triggers that don't need to fire
        if (TRIGGER_FIRED_BY_UPDATE(event) || TRIGGER_FIRED_BY_DELETE(event)) {
            switch (RI_FKey_trigger_type(trigger->tgfoid)) {
                case RI_TRIGGER_PK:
                    if (!RI_FKey_pk_upd_check_required(trigger, rel, oldslot, newslot))
                        continue;
                    break;
                case RI_TRIGGER_FK:
                    if (!RI_FKey_fk_upd_check_required(trigger, rel, oldslot, newslot))
                        continue;
                    break;
            }
        }

        // Set up shared event data and add to queue
        new_shared.ats_event = (event & TRIGGER_EVENT_OPMASK) |
                              (row_trigger ? TRIGGER_EVENT_ROW : 0) |
                              (trigger->tgdeferrable ? AFTER_TRIGGER_DEFERRABLE : 0) |
                              (trigger->tginitdeferred ? AFTER_TRIGGER_INITDEFERRED : 0);
        new_shared.ats_tgoid = trigger->tgoid;
        new_shared.ats_relid = RelationGetRelid(rel);
        new_shared.ats_firing_id = 0;
        new_shared.ats_table = (trigger->tgoldtable || trigger->tgnewtable) ?
                              transition_capture->tcs_private : NULL;
        new_shared.ats_modifiedcols = modifiedCols;

        afterTriggerAddEvent(&afterTriggers.query_stack[afterTriggers.query_depth].events,
                           &new_event, &new_shared);
    }
}
```