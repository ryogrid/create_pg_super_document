# EventTriggerInhibitCommandCollection

## Location
[src/backend/commands/event_trigger.c:1554-1565](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1554-L1565)

## Overview
Inhibits DDL command collection for event triggers by setting a flag in the current event trigger state.

## Definition

```c
void
EventTriggerInhibitCommandCollection(void)
```
## Detailed Description
This function is part of PostgreSQL's event trigger DDL command collection system. It temporarily disables the collection of DDL commands by setting the  flag to true in the current event trigger state. This is used when certain DDL operations need to be excluded from event trigger processing or when nested DDL commands should not be collected to avoid redundant or problematic trigger executions.

The function performs a safety check by returning early if  is NULL, ensuring it only operates when event triggers are actually active.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - currentEventTriggerState (global state variable)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1687)
  - CALLED_AS_EVENT_TRIGGER macro context

## Notes and Other Information
- This function is part of the DDL command collection framework that supports event triggers
- It works in conjunction with EventTriggerUndoInhibitCommandCollection to provide temporary inhibition of command collection
- The inhibition affects the collection of DDL commands that would otherwise be made available to event trigger functions via pg_event_trigger_ddl_commands()
- [Command](../C/Command.md) collection can be restored by calling EventTriggerUndoInhibitCommandCollection

## Simplified Source

```c
void EventTriggerInhibitCommandCollection(void)
{
    // Skip if event trigger state not active
    if (!currentEventTriggerState)
        return;

    // Disable command collection
    currentEventTriggerState->commandCollectionInhibited = true;
}
```