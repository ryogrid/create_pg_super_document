# RunObjectTruncateHookStr

## Location
[src/backend/catalog/objectaccess.c:202-217](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaccess.c#L202-L217)

## Overview
RunObjectTruncateHookStr is a function that triggers object access hook callbacks for table truncate operations using string-based object names, allowing extensions to be notified when tables are being truncated.

## Definition
void RunObjectTruncateHookStr(const char *objectName)

## Detailed Description
This function serves as the entry point for OAT_TRUNCATE (Object Access Type Truncate) events in PostgreSQL's string-based object access hook system. It is called when tables are being truncated to notify registered extensions that can perform additional processing such as logging, auditing, cleanup of related data structures, or validation of truncate permissions.

The function is specifically designed for table truncation operations and uses RelationRelationId as the fixed catalog class identifier, indicating that the operation is always related to relations (tables). Unlike other hook functions that may handle various object types, this function is specialized for truncate operations which are primarily applicable to tables and table-like objects.

## Parameters / Member Variables
- objectName: The string name of the table/relation that is being truncated

## Dependencies
- Functions called/Symbols referenced:
  - object_access_hook_str (global function pointer)
  - OAT_TRUNCATE (object access type constant)
  - RelationRelationId (system catalog relation OID for pg_class)
  - Assert (debugging assertion macro)

- Called from (representative examples):
  - InvokeObjectTruncateHookStr
  - [ObjectAccessNamespaceSearch](../O/ObjectAccessNamespaceSearch.md)

## Notes and Other Information
- This is part of the string-based variant of PostgreSQL's object access hook system specialized for TRUNCATE operations
- The function uses a hardcoded RelationRelationId since truncate operations are specific to relations/tables
- No additional auxiliary data is passed (NULL parameter) as truncate operations typically do not require complex metadata
- The subId parameter is set to 0 since truncate operations apply to the entire table rather than specific sub-components
- This hook is commonly used by auditing extensions, replication systems, and monitoring tools to track when tables are truncated
- Extensions can use this hook to perform cleanup of associated indexes, triggers, or external data structures before or after truncation
- The string-based approach allows extensions to work with table names directly without needing to resolve OIDs

## Simplified Source

```c
void
RunObjectTruncateHookStr(const char *objectName)
{
    // Ensure string-based hook is registered (caller should check this)
    Assert(object_access_hook_str != NULL);

    // Call the registered string-based object access hook for truncate event
    // Uses RelationRelationId as classId, 0 as subId, NULL as auxiliary data
    (*object_access_hook_str)(OAT_TRUNCATE, RelationRelationId, objectName, 0, NULL);
}
```