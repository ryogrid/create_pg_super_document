# EventTriggerDDLCommandStart

## Location
[src/backend/commands/event_trigger.c:721-771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L721-L771)

## Overview
EventTriggerDDLCommandStart fires ddl_command_start event triggers when a DDL command begins execution, providing a hook for monitoring and potentially modifying DDL operations.

## Definition
```c
void EventTriggerDDLCommandStart(Node *parsetree)
```

## Detailed Description
This function is responsible for firing ddl_command_start event triggers at the beginning of DDL command execution. It serves as a critical integration point in PostgreSQL's event trigger system, allowing custom functions to execute before DDL operations proceed.

The function implements several safety mechanisms:
- Event triggers are completely disabled in standalone mode to prevent recovery scenarios from being compromised by broken triggers
- Relies on the event_triggers GUC setting which can be disabled by superusers for easier database repair
- Uses EventTriggerCommonSetup to identify applicable triggers based on the parse tree and event type
- Ensures trigger changes are visible to the main command through CommandCounterIncrement

The function follows a standard pattern: setup eligible triggers, invoke them, clean up resources, and ensure visibility of any changes made by the triggers.

## Parameters / Member Variables
- `parsetree`: Node pointer representing the parsed DDL command that triggered this event

## Dependencies
- Functions called/Symbols referenced:
  - EventTriggerData (struct for trigger context)
  - [EventTriggerCommonSetup](EventTriggerCommonSetup.md) (identifies applicable triggers)
  - EVT_DDLCommandStart (event type constant)
  - [EventTriggerInvoke](EventTriggerInvoke.md) (executes the triggers)
  - [list_free](../l/list_free.md) (memory cleanup)
  - CommandCounterIncrement (ensures visibility)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (main DDL command processing)

## Notes and Other Information
- Event triggers are disabled in standalone mode for disaster recovery purposes
- The function prevents execution if pg_event_trigger indexes are damaged by requiring normal postmaster operation
- Uses a superuser-controllable GUC (event_triggers) as an additional safety mechanism
- Changes made by event triggers are made visible to the main command through CommandCounterIncrement
- Part of PostgreSQL's comprehensive event trigger system for DDL monitoring and customization