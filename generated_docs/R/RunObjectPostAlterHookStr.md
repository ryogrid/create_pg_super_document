# RunObjectPostAlterHookStr

## Location
[src/backend/catalog/objectaccess.c:218-240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaccess.c#L218-L240)

## Overview
RunObjectPostAlterHookStr is a function that triggers object access hook callbacks for post-alter events using string-based object names, allowing extensions to be notified after database objects have been altered.

## Definition
void RunObjectPostAlterHookStr(Oid classId, const char *objectName, int subId, Oid auxiliaryId, bool is_internal)

## Detailed Description
This function serves as the entry point for OAT_POST_ALTER (Object Access Type Post Alter) events in PostgreSQL's string-based object access hook system. It is called after database objects have been successfully altered to notify registered extensions that can perform additional processing such as logging, auditing, cache invalidation, replication coordination, or other post-alteration tasks.

The function differs from its OID-based counterpart by using object names (strings) rather than OIDs to identify objects. It prepares an ObjectAccessPostAlter structure containing metadata about the alteration event, including auxiliary object information and whether the alteration was internal to PostgreSQL operations, and passes this information to registered string-based hook functions via the global object_access_hook_str function pointer.

## Parameters / Member Variables
- classId: The OID of the system catalog relation that contains the altered object
- objectName: The string name of the object that was altered
- subId: Sub-object identifier for objects that have sub-components (e.g., column number for table columns)
- auxiliaryId: Additional OID providing context about related objects involved in the alteration
- is_internal: Boolean flag indicating whether this object alteration is internal to PostgreSQL operations or user-initiated

## Dependencies
- Functions called/Symbols referenced:
  - object_access_hook_str (global function pointer)
  - [ObjectAccessPostAlter](../O/ObjectAccessPostAlter.md) (structure type)
  - OAT_POST_ALTER (object access type constant)
  - Assert (debugging assertion macro)
  - memset (memory initialization function)

- Called from (representative examples):
  - InvokeObjectPostAlterHookArgStr
  - [ObjectAccessNamespaceSearch](../O/ObjectAccessNamespaceSearch.md)

## Notes and Other Information
- This is part of the string-based variant of PostgreSQL's object access hook system for ALTER operations
- The function initializes an ObjectAccessPostAlter structure with zeroed memory and sets both the auxiliary_id and is_internal fields based on the parameters
- The auxiliaryId parameter provides additional context about related objects involved in complex alter operations
- [String](../S/String.md)-based alter hooks are particularly useful for extensions that prefer to work with object names rather than OIDs
- The is_internal flag helps extensions distinguish between user-initiated alterations and internal PostgreSQL operations
- This hook is commonly used by logical replication systems, auditing extensions, schema change tracking tools, and cache invalidation mechanisms
- Extensions can use this hook to update external metadata, invalidate cached information, or trigger related operations after schema changes