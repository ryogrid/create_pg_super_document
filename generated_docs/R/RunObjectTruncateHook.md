# RunObjectTruncateHook

## Location
[src/backend/catalog/objectaccess.c:76-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaccess.c#L76-L91)

## Overview
Executes registered object access hooks for truncate events, allowing extensions to perform actions when a PostgreSQL table is truncated.

## Definition
```c
void RunObjectTruncateHook(Oid objectId)
```

## Detailed Description
This function serves as the entry point for the OAT_TRUNCATE object access hook event. It is called when a table is being truncated in PostgreSQL. Unlike other object access hooks, this function is specifically designed for table truncation operations and has a simplified parameter set since truncate operations only apply to relations (tables).

The function directly invokes any registered object access hooks through the global object_access_hook function pointer, passing the OAT_TRUNCATE event type. It uses RelationRelationId as the classId since truncate operations are specific to relations in the pg_class catalog, and passes NULL as the auxiliary data since no additional context structure is needed for truncate events.

## Parameters / Member Variables
- `objectId`: The OID of the relation (table) being truncated

## Dependencies
- Functions called/Symbols referenced:
  - OAT_TRUNCATE (enum value)
  - RelationRelationId (constant - OID of pg_class catalog)
  - object_access_hook (global function pointer)
  - Assert (assertion macro)

- Called from (representative examples):
  - [ObjectAccessNamespaceSearch](../O/ObjectAccessNamespaceSearch.md)  
  - InvokeObjectTruncateHook

## Notes and Other Information
- This is the simplest of the object access hook functions, taking only the objectId parameter
- Always uses RelationRelationId (1259) as the classId since truncate operations only apply to relations
- Always uses 0 as subId since truncate operations apply to entire tables, not sub-objects
- Passes NULL as auxiliary data since no additional context is needed for truncate events
- The function includes an assertion to ensure object_access_hook is not NULL
- Part of PostgreSQL's extensibility framework, allowing extensions to implement audit logging or security policies for truncate operations
- Unlike DROP operations, TRUNCATE operations are typically faster and don't trigger referential integrity checks

## Simplified Source

```c
void
RunObjectTruncateHook(Oid objectId)
{
    // Ensure hook is registered (caller should check this)
    Assert(object_access_hook != NULL);

    // Call the registered object access hook for truncate event
    // Uses RelationRelationId as classId, 0 as subId, NULL as auxiliary data
    (*object_access_hook)(OAT_TRUNCATE, RelationRelationId, objectId, 0, NULL);
}
```