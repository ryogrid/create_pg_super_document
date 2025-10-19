# RunObjectDropHookStr

## Location
[src/backend/catalog/objectaccess.c:180-201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaccess.c#L180-L201)

## Overview
RunObjectDropHookStr is a function that triggers object access hook callbacks for object drop events using string-based object names, allowing extensions to be notified when database objects are being dropped.

## Definition
void RunObjectDropHookStr(Oid classId, const char *objectName, int subId, int dropflags)

## Detailed Description
This function serves as the entry point for OAT_DROP (Object Access Type Drop) events in PostgreSQL's string-based object access hook system. It is called when database objects are being dropped to notify registered extensions that can perform additional processing such as cleanup operations, logging, auditing, dependency tracking, or cascading operations.

The function differs from its OID-based counterpart by using object names (strings) rather than OIDs to identify objects being dropped. It prepares an ObjectAccessDrop structure containing metadata about the drop operation, particularly the drop flags that specify the nature of the drop operation (CASCADE, RESTRICT, etc.), and passes this information to registered string-based hook functions via the global object_access_hook_str function pointer.

## Parameters / Member Variables
- classId: The OID of the system catalog relation that contains the object being dropped
- objectName: The string name of the object that is being dropped
- subId: Sub-object identifier for objects that have sub-components (e.g., column number for table columns)
- dropflags: Integer flags specifying the nature of the drop operation (e.g., CASCADE, RESTRICT behavior)

## Dependencies
- Functions called/Symbols referenced:
  - object_access_hook_str (global function pointer)
  - [ObjectAccessDrop](../O/ObjectAccessDrop.md) (structure type)
  - OAT_DROP (object access type constant)
  - Assert (debugging assertion macro)
  - memset (memory initialization function)

- Called from (representative examples):
  - InvokeObjectDropHookArgStr
  - [ObjectAccessNamespaceSearch](../O/ObjectAccessNamespaceSearch.md)

## Notes and Other Information
- This is part of the string-based variant of PostgreSQL's object access hook system for drop operations
- The function initializes an ObjectAccessDrop structure with zeroed memory and sets the dropflags field based on the parameter
- Drop flags typically include information about CASCADE/RESTRICT behavior and other drop operation modifiers
- [String](../S/String.md)-based drop hooks are particularly useful for extensions that need to track object names during drop operations
- This hook is commonly used by logical replication systems, auditing extensions, and dependency tracking tools that need to respond to schema changes
- Extensions can use this hook to perform cleanup operations, validate drop permissions, or maintain external metadata structures

## Simplified Source

```c
void RunObjectDropHookStr(Oid classId, const char *objectName, int subId,
                         int dropflags)
{
    ObjectAccessDrop drop_arg;

    // Initialize structure with drop operation metadata
    memset(&drop_arg, 0, sizeof(ObjectAccessDrop));
    drop_arg.dropflags = dropflags;

    // Call registered string-based object access hook for drop event
    (*object_access_hook_str)(OAT_DROP,
                             classId, objectName, subId,
                             (void *) &drop_arg);
}
```