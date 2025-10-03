# cancel_prior_stmt_triggers

## Location
[src/backend/commands/trigger.c:6591-6665](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L6591-L6665)

## Overview
Cancels previously queued AFTER STATEMENT triggers for a specific relation and operation to ensure proper trigger firing order and prevent duplicate executions when multiple sub-statements affect the same table.

## Definition

```c
static void
cancel_prior_stmt_triggers(Oid relid, CmdType cmdType, int tgevent)
```
## Detailed Description
This function addresses a complex challenge in PostgreSQL's trigger system: maintaining the proper firing order and frequency of AFTER STATEMENT triggers when multiple operations or sub-statements affect the same table within a single query context. The SQL standard requires that AFTER ROW triggers fire before AFTER STATEMENT triggers, and that statement triggers fire only once per statement.

The function operates by scanning through previously queued trigger events and marking matching AFTER STATEMENT triggers as DONE (cancelled). This allows new statement triggers to be queued that will fire after any row-level triggers that may have been queued by the current sub-statement, preserving the required firing order.

**Key Design Elements:**

1. **State Management**: Uses the same AfterTriggersTableData structure that holds transition tables, ensuring that if new transition tables are created due to additional tuple processing, new statement triggers can be queued without interfering with previous ones.

2. **Selective Cancellation**: Only cancels triggers that match the exact relation, operation type, and are AFTER STATEMENT triggers. This precision ensures that unrelated triggers remain unaffected.

3. **Position Tracking**: Saves the current event list location so future invocations can efficiently locate and cancel the triggers being queued, avoiding the need to scan the entire event list.

4. **Transition Table Considerations**: The design acknowledges that if AFTER ROW triggers are using transition tables, changing those tables after triggers have seen them could cause incorrect behavior. In such cases, the system creates new transition tables and allows new statement trigger firings.

The function is particularly important in scenarios involving foreign key enforcement, where multiple FK triggers might sequentially queue triggers for the same table within the same trigger query level.

## Parameters / Member Variables
- `relid`: The OID of the relation for which to cancel statement triggers
- `cmdType`: The command type (INSERT, UPDATE, DELETE) for which to cancel triggers
- `tgevent`: The specific trigger event type to match when cancelling
## Dependencies
- Functions called/Symbols referenced:
  - [GetAfterTriggersTableData](../G/GetAfterTriggersTableData.md) (retrieves table data for the relation/command)
  - for_each_chunk_from (macro for iterating through event chunks from a position)
  - for_each_event_from (macro for iterating through events from a position)
  - GetTriggerSharedData (retrieves shared trigger data from event)
  - TRIGGER_FIRED_FOR_STATEMENT (macro to check if trigger is statement-level)
  - TRIGGER_FIRED_AFTER (macro to check if trigger is AFTER trigger)
- Called from (representative examples):
  - [AfterTriggerSaveEvent](../A/AfterTriggerSaveEvent.md) (when queuing statement-level triggers for INSERT, UPDATE, DELETE)

## Notes and Other Information
- The function implements a sophisticated balancing act between trigger firing order requirements and performance considerations
- The cancellation mechanism only affects triggers that haven't been fired yet, preserving already-executed triggers
- The position-tracking optimization reduces the cost of repeated cancellations within the same query context
- The design handles edge cases where transition tables are involved, ensuring data consistency for triggers that depend on transition table contents
- This function is a key component in preventing the "odd behavior" that could occur when multiple FK enforcement triggers operate on the same table
- The function marks cancelled triggers with AFTER_TRIGGER_DONE flag and clears any AFTER_TRIGGER_IN_PROGRESS flag
- Works in conjunction with the before_stmt_triggers_fired function to maintain comprehensive statement trigger semantics

## Simplified Source

```c
static void
cancel_prior_stmt_triggers(Oid relid, CmdType cmdType, int tgevent) {
    AfterTriggersTableData *table;
    AfterTriggersQueryData *qs = &afterTriggers.query_stack[afterTriggers.query_depth];

    // Get table data for this relation/command
    table = GetAfterTriggersTableData(relid, cmdType);

    // If we have previously queued statement triggers, cancel them
    if (table->after_trig_done) {
        AfterTriggerEvent event;
        AfterTriggerEventChunk *chunk;

        // Start scanning from saved position or current head
        if (table->after_trig_events.tail) {
            chunk = table->after_trig_events.tail;
            event = (AfterTriggerEvent) table->after_trig_events.tailfree;
        } else {
            chunk = qs->events.head;
            event = NULL;
        }

        // Scan through events and cancel matching AFTER STATEMENT triggers
        for_each_chunk_from(chunk) {
            if (event == NULL)
                event = (AfterTriggerEvent) CHUNK_DATA_START(chunk);

            for_each_event_from(event, chunk) {
                AfterTriggerShared evtshared = GetTriggerSharedData(event);

                // Stop if this event doesn't match our criteria
                if (evtshared->ats_relid != relid ||
                    (evtshared->ats_event & TRIGGER_EVENT_OPMASK) != tgevent ||
                    !TRIGGER_FIRED_FOR_STATEMENT(evtshared->ats_event) ||
                    !TRIGGER_FIRED_AFTER(evtshared->ats_event))
                    goto done;

                // Mark the trigger as done (cancelled)
                event->ate_flags &= ~AFTER_TRIGGER_IN_PROGRESS;
                event->ate_flags |= AFTER_TRIGGER_DONE;
            }
            event = NULL; // Reset for next chunk
        }
    }

done:
    // Save current position for next invocation
    table->after_trig_done = true;
    table->after_trig_events = qs->events;
}
```