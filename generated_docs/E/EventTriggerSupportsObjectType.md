# EventTriggerSupportsObjectType

## Location
src/backend/commands/event_trigger.c: 1134 - 1157

## Overview
EventTriggerSupportsObjectType determines whether event triggers are supported for a specific database object type, filtering out global objects and self-referential cases.

## Definition
```c
bool EventTriggerSupportsObjectType(ObjectType obtype)
```

## Detailed Description
This function implements a filtering mechanism to determine which database object types can have event triggers associated with them. It uses a switch statement to explicitly exclude certain object types from event trigger support:

1. **Global Objects**: Database, tablespace, role, and parameter ACL objects are excluded because they operate at a cluster level rather than within a specific database context where event triggers operate.
2. **Self-Reference Prevention**: Event triggers themselves cannot have event triggers to prevent recursive complications.
3. **Default Support**: All other object types are supported by default.

The function serves as a gatekeeper in the event trigger system, ensuring that event triggers are only applied to appropriate database objects that exist within the proper scope.

## Parameters / Member Variables
- `obtype`: ObjectType enum value representing the type of database object to check for event trigger support

## Dependencies
- Functions called/Symbols referenced:
  - ObjectType (enum type)
  - OBJECT_DATABASE, OBJECT_TABLESPACE, OBJECT_ROLE, OBJECT_PARAMETER_ACL, OBJECT_EVENT_TRIGGER (enum constants)
- Called from:
  - [ExecGrantStmt_oids](ExecGrantStmt_oids.md) (ACL operations)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (multiple locations for utility command processing)
  - CALLED_AS_EVENT_TRIGGER (macro usage)

## Notes and Other Information
- Returns false for global objects that exist outside the database scope where event triggers operate
- Prevents infinite recursion by disallowing event triggers on event triggers themselves
- Referenced in the PostgreSQL documentation's event trigger support matrix
- Used extensively in utility command processing to determine when to fire event triggers
- The function's logic aligns with PostgreSQL's architecture where event triggers are database-scoped rather than cluster-scoped