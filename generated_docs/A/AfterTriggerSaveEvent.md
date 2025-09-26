# AfterTriggerSaveEvent

## Location
src/backend/commands/trigger.c: 6148 - 6544

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
  - AfterTriggerEnlargeQueryState (ensures adequate query depth storage)
  - GetAfterTriggersTransitionTable (manages transition table creation)
  - TransitionTableAddTuple (adds tuples to transition tables)
  - cancel_prior_stmt_triggers (cancels previous statement-level triggers)
  - execute_attr_map_slot (converts tuples between partition formats)
  - ExecGetTriggerOldSlot/ExecGetTriggerNewSlot (gets trigger tuple slots)
  - TriggerEnabled (checks if trigger should fire)
  - RI_FKey_trigger_type (identifies foreign key trigger types)
  - afterTriggerAddEvent (adds event to trigger queue)
- Called from (representative examples):
  - ExecASInsertTriggers (for INSERT statement-level triggers)
  - ExecARInsertTriggers (for INSERT row-level triggers)
  - ExecASDeleteTriggers (for DELETE statement-level triggers)
  - ExecARDeleteTriggers (for DELETE row-level triggers)
  - ExecASUpdateTriggers (for UPDATE statement-level triggers)
  - ExecARUpdateTriggers (for UPDATE row-level triggers)
  - ExecASTruncateTriggers (for TRUNCATE triggers)

## Notes and Other Information
- The function is called even when no triggers exist for an event if transition tables need to be built for statement-level triggers
- Contains special handling for foreign tables using FDW-specific tuple storage mechanisms
- Implements complex optimization logic for foreign key constraints to avoid unnecessary trigger executions
- The cross-partition update logic ensures that foreign key constraints work correctly when rows move between partitions
- Statement-level triggers are designed to fire exactly once per statement, after all row-level triggers have been processed
- Transition tables are built immediately to support AFTER ROW triggers that need to query transition data