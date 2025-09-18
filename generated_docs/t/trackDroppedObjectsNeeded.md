# trackDroppedObjectsNeeded

## Location
src/backend/commands/event_trigger.c: 1246 - 1277

## Overview
A utility function that determines whether PostgreSQL needs to track objects being dropped, which is necessary for proper event trigger processing.

## Definition


## Detailed Description
This function checks if any event triggers are registered for SQL DROP, table rewrite, or DDL command end events. It serves as an optimization mechanism to avoid the overhead of tracking dropped objects when no relevant event triggers are active. The function is essential for the event trigger system's performance, as tracking dropped objects incurs computational costs that should only be paid when necessary.

The function examines three types of events:
- SQL Drop events (EVT_SQLDrop): Triggered when objects are dropped
- Table Rewrite events (EVT_TableRewrite): Triggered during table rewrites
- DDL Command End events (EVT_DDLCommandEnd): Triggered at the end of DDL commands

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - EventCacheLookup (called 3 times to check for different event types)
  - EVT_SQLDrop (constant for SQL drop events)
  - EVT_TableRewrite (constant for table rewrite events) 
  - EVT_DDLCommandEnd (constant for DDL command end events)
- Called from (representative examples):
  - deleteObjectsInList (src/backend/catalog/dependency.c:193)
  - EventTriggerBeginCompleteQuery (src/backend/commands/event_trigger.c:1194)

## Notes and Other Information
- Returns true if any of the three event trigger types exist, false otherwise
- Used as a performance optimization to avoid unnecessary object tracking overhead
- The function uses bitwise OR (||) to check all three event types
- Located in src/backend/commands/event_trigger.c:1246-1277