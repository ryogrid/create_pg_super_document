# EventTriggerUndoInhibitCommandCollection

## Location
[src/backend/commands/event_trigger.c:1566-1587](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1566-L1587)

## Overview
Re-establishes DDL command collection for event triggers by clearing the inhibition flag in the current event trigger state.

## Definition
```c
void EventTriggerUndoInhibitCommandCollection(void)
```

## Detailed Description
This function reverses the effect of EventTriggerInhibitCommandCollection by setting the `commandCollectionInhibited` flag to false in the current event trigger state. This re-enables the collection of DDL commands for event trigger processing after they were temporarily disabled.

The function includes the same safety check as its counterpart, returning early if `currentEventTriggerState` is NULL to ensure it only operates when event triggers are active.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - currentEventTriggerState (global state variable)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1695)
  - CALLED_AS_EVENT_TRIGGER macro context

## Notes and Other Information
- This function works as a pair with EventTriggerInhibitCommandCollection to provide temporary control over DDL command collection
- Used to restore normal event trigger command collection after a period of inhibition
- Essential for ensuring that DDL commands are properly collected and made available to event trigger functions when appropriate
- Part of the broader DDL command collection framework that supports pg_event_trigger_ddl_commands()

## Simplified Source

```c
void EventTriggerUndoInhibitCommandCollection(void)
{
    // Skip if event trigger state not active
    if (!currentEventTriggerState)
        return;

    // Re-enable command collection
    currentEventTriggerState->commandCollectionInhibited = false;
}
```