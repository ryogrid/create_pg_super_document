# EventTriggerSQLDrop

## Location
[src/backend/commands/event_trigger.c:820-892](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L820-L892)

## Overview
EventTriggerSQLDrop fires sql_drop event triggers when objects are dropped during DDL operations, providing access to information about dropped objects before they are permanently removed.

## Definition
```c
void EventTriggerSQLDrop(Node *parsetree)
```

## Detailed Description
This function is responsible for firing sql_drop event triggers during DDL operations that result in object drops. It provides a specialized mechanism for capturing information about objects being dropped before they are permanently removed from the system catalog.

Key features and behaviors:
- Checks for both general event trigger enablement and specific sql_drop trigger availability
- Validates that the SQLDropList contains objects before proceeding (dropped object collection is disabled if no sql_drop triggers exist)
- Uses a protected execution model with PG_TRY/PG_FINALLY to ensure the in_sql_drop flag is properly reset even if triggers fail
- Sets the in_sql_drop flag during trigger execution to enable pg_event_trigger_dropped_objects function to work correctly
- Ensures main command changes are visible to triggers before execution

The function is more complex than other event trigger functions due to the need to manage the dropped objects list and provide safe access to it through the in_sql_drop state flag.

## Parameters / Member Variables
- `parsetree`: Node pointer representing the parsed DDL command that is causing object drops

## Dependencies
- Functions called/Symbols referenced:
  - EventTriggerData (struct for trigger context)
  - [slist_is_empty](../s/slist_is_empty.md) (checks if SQLDropList is empty)
  - [EventTriggerCommonSetup](EventTriggerCommonSetup.md) (identifies applicable triggers)
  - EVT_SQLDrop (event type constant)
  - CommandCounterIncrement (ensures visibility)
  - PG_TRY/PG_FINALLY/PG_END_TRY (exception handling)
  - [EventTriggerInvoke](EventTriggerInvoke.md) (executes the triggers)
  - [list_free](../l/list_free.md) (memory cleanup)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (main DDL command processing)

## Notes and Other Information
- Includes specialized logic for dropped object collection and access control
- The in_sql_drop flag enables pg_event_trigger_dropped_objects to function properly
- Uses exception handling to ensure state cleanup even if triggers fail
- Object collection is automatically disabled when no sql_drop triggers are defined for performance
- Part of PostgreSQL's event trigger system specifically designed for object lifecycle monitoring
- The SQLDropList validation prevents unnecessary execution when no objects are being dropped