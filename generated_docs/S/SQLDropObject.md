# SQLDropObject

## Location
[src/backend/commands/event_trigger.c:87-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L87-L100)

## Overview
SQLDropObject is a struct that represents metadata about database objects that have been dropped, used by PostgreSQL's event trigger system to provide detailed information about dropped objects to event trigger functions.

## Definition
```c
typedef struct SQLDropObject
{
    ObjectAddress address;
    const char *schemaname;
    const char *objname;
    const char *objidentity;
    const char *objecttype;
    List       *addrnames;
    List       *addrargs;
    bool        original;
    bool        normal;
    bool        istemp;
    slist_node  next;
} SQLDropObject;
```

## Detailed Description
SQLDropObject serves as a comprehensive descriptor for database objects that have been dropped during DDL operations. This structure is fundamental to PostgreSQL's event trigger system, specifically for `sql_drop` event triggers that need detailed information about what objects were dropped.

The structure captures both the technical identifiers (ObjectAddress) and human-readable descriptions of dropped objects. It maintains sufficient information to allow event trigger functions to understand exactly what was dropped, including schema qualification, object names, and various object properties. The structure is designed to be part of a singly-linked list for efficient collection and iteration over multiple dropped objects within a single command.

## Parameters / Member Variables
- `address`: ObjectAddress containing the class ID, object ID, and sub-object ID of the dropped object
- `schemaname`: Name of the schema containing the dropped object (NULL if not applicable)
- `objname`: Simple name of the dropped object
- `objidentity`: Complete identity string for the dropped object, including schema qualification when needed
- `objecttype`: String representation of the object type (e.g., "table", "index", "function")
- `addrnames`: List of strings representing the address names for complex object identification
- `addrargs`: List of strings representing additional arguments needed for object identification
- `original`: Boolean flag indicating whether this object was explicitly specified in the DROP command (true) or dropped as a dependency (false)
- `normal`: Boolean flag indicating whether this is a normal object (true) or an internal/system object (false)
- `istemp`: Boolean flag indicating whether the dropped object was a temporary object
- `next`: Singly-linked list node for chaining multiple SQLDropObject instances together

## Dependencies
- Functions called/Symbols referenced:
  - [ObjectAddress](../O/ObjectAddress.md) (PostgreSQL object addressing structure)
  - [List](../L/List.md) (PostgreSQL list type)
  - [slist_node](../s/slist_node.md) (PostgreSQL singly-linked list node)

- Called from (representative examples):
  - [EventTriggerSQLDropAddObject](../E/EventTriggerSQLDropAddObject.md) (src/backend/commands/event_trigger.c:1280, 1296)
  - [pg_event_trigger_dropped_objects](../p/pg_event_trigger_dropped_objects.md) (src/backend/commands/event_trigger.c:1417, 1422)

## Notes and Other Information
This structure is central to the `sql_drop` event trigger functionality in PostgreSQL. It provides event trigger functions with comprehensive information about dropped objects through the `pg_event_trigger_dropped_objects()` function. The distinction between `original` and dependent objects allows event triggers to understand the cascade effects of DROP operations. The structure is part of the event trigger infrastructure defined in src/backend/commands/event_trigger.c and is used to build detailed drop reports for event trigger processing. The `objidentity` field is particularly important as it provides a complete, unambiguous identifier that can be used to reconstruct or reference the dropped object in logs or external systems.