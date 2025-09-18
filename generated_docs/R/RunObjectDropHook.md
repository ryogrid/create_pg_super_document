# RunObjectDropHook

## Location
[src/backend/catalog/objectaccess.c:54-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaccess.c#L54-L75)

## Overview
Executes registered object access hooks for drop events, allowing extensions to perform actions before a PostgreSQL database object is dropped.

## Definition
```c
void RunObjectDropHook(Oid classId, Oid objectId, int subId, int dropflags)
```

## Detailed Description
This function serves as the entry point for the OAT_DROP object access hook event. It is called when a database object (such as a table, index, function, etc.) is about to be dropped from the PostgreSQL system catalogs. The function provides a standardized way for extensions to hook into the object deletion process and perform additional processing, validation, or cleanup operations.

The function constructs an ObjectAccessDrop structure with the provided drop flags and invokes any registered object access hooks through the global object_access_hook function pointer. This enables extensions to extend PostgreSQL's functionality by responding to object deletion events, potentially implementing custom security policies, audit logging, or cleanup procedures.

## Parameters / Member Variables
- `classId`: The OID of the system catalog (pg_class, pg_proc, etc.) containing the object to be dropped
- `objectId`: The OID of the object being dropped within the specified catalog
- `subId`: Sub-object identifier (e.g., column number for table columns, 0 for whole objects)  
- `dropflags`: Integer flags providing context about the deletion operation (see PERFORM_DELETION_* constants in dependency.h)

## Dependencies
- Functions called/Symbols referenced:
  - [ObjectAccessDrop](../O/ObjectAccessDrop.md) (struct type)
  - OAT_DROP (enum value)
  - object_access_hook (global function pointer)
  - Assert (assertion macro)
  - memset (memory initialization function)

- Called from (representative examples):
  - [ObjectAccessNamespaceSearch](../O/ObjectAccessNamespaceSearch.md)
  - InvokeObjectDropHookArg

## Notes and Other Information
- The function includes an assertion to ensure object_access_hook is not NULL, though the comment suggests callers should verify this condition
- The ObjectAccessDrop structure is zero-initialized before setting the dropflags field
- The dropflags parameter provides important context about the type of deletion being performed (cascade, restrict, etc.)
- This hook is called before the actual deletion occurs, allowing extensions to potentially prevent or modify the operation
- Part of PostgreSQL's extensibility framework for implementing custom security policies and audit systems