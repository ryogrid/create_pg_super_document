# EventTriggerSupportsObject

## Location
[src/backend/commands/event_trigger.c:1158-1183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1158-L1183)

## Overview
EventTriggerSupportsObject determines whether event triggers are supported for a specific database object instance by examining its object class ID, filtering out global objects and self-referential cases.

## Definition
```c
bool EventTriggerSupportsObject(const ObjectAddress *object)
```

## Detailed Description
This function provides a more granular check than EventTriggerSupportsObjectType by examining the actual ObjectAddress of a specific database object. It uses the classId field of the ObjectAddress to determine support:

1. **Global Object Classes**: Objects from relation classes like DatabaseRelationId, TableSpaceRelationId, AuthIdRelationId, AuthMemRelationId, and ParameterAclRelationId are excluded because they represent cluster-wide objects that operate outside the database scope where event triggers function.

2. **Self-Reference Prevention**: EventTriggerRelationId objects (event triggers themselves) are excluded to prevent recursive event trigger scenarios.

3. **Default Support**: All other object classes are supported by default.

The function operates on ObjectAddress structures, which contain the precise identification of database objects including their class, OID, and sub-object information.

## Parameters / Member Variables
- `object`: Pointer to const ObjectAddress structure containing:
  - `classId`: OID of the system catalog containing the object
  - `objectId`: OID of the object within its catalog  
  - `objectSubId`: Sub-object identifier (e.g., column number)

## Dependencies
- Functions called/Symbols referenced:
  - DatabaseRelationId, TableSpaceRelationId, AuthIdRelationId, AuthMemRelationId, ParameterAclRelationId, EventTriggerRelationId (system catalog relation OID constants)
- Called from:
  - [deleteObjectsInList](../d/deleteObjectsInList.md) (dependency management during object deletion)
  - [EventTriggerSQLDropAddObject](EventTriggerSQLDropAddObject.md) (SQL DROP event processing)
  - CALLED_AS_EVENT_TRIGGER (macro usage)

## Notes and Other Information
- Works with concrete ObjectAddress instances rather than abstract object types
- Complements EventTriggerSupportsObjectType by providing instance-level filtering
- Used in dependency management and SQL DROP event processing
- The exclusion of global objects reflects PostgreSQL's architecture where event triggers are database-scoped
- Referenced in PostgreSQL documentation's event trigger support matrix
- Critical for preventing event triggers from firing inappropriately on cluster-level operations

## Simplified Source

```c
bool EventTriggerSupportsObject(const ObjectAddress *object) {
    // Check if object class is globally scoped (no event trigger support)
    switch (object->classId) {
        case DatabaseRelationId:
        case TableSpaceRelationId:
        case AuthIdRelationId:
        case AuthMemRelationId:
        case ParameterAclRelationId:
            return false;  // Global objects not supported

        case EventTriggerRelationId:
            return false;  // Prevent recursive event triggers

        default:
            return true;   // All other object classes supported
    }
}
```