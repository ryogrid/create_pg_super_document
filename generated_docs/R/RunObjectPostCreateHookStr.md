# RunObjectPostCreateHookStr

## Location
src/backend/catalog/objectaccess.c: 158 - 179

## Overview
RunObjectPostCreateHookStr is a function that triggers object access hook callbacks for post-creation events using string-based object names, allowing extensions to be notified after database objects have been created.

## Definition
void RunObjectPostCreateHookStr(Oid classId, const char *objectName, int subId, bool is_internal)

## Detailed Description
This function serves as the entry point for OAT_POST_CREATE (Object Access Type Post Create) events in PostgreSQL's string-based object access hook system. It is called after database objects have been successfully created to notify registered extensions that can perform additional processing such as logging, auditing, replication setup, or other post-creation tasks.

The function differs from its OID-based counterpart by using object names (strings) rather than OIDs to identify objects. It prepares an ObjectAccessPostCreate structure containing metadata about the creation event, particularly whether the creation was internal to PostgreSQL operations, and passes this information to registered string-based hook functions via the global object_access_hook_str function pointer.

## Parameters / Member Variables
- classId: The OID of the system catalog relation that contains the created object
- objectName: The string name of the object that was created
- subId: Sub-object identifier for objects that have sub-components (e.g., column number for table columns)
- is_internal: Boolean flag indicating whether this object creation is internal to PostgreSQL operations or user-initiated

## Dependencies
- Functions called/Symbols referenced:
  - object_access_hook_str (global function pointer)
  - ObjectAccessPostCreate (structure type)
  - OAT_POST_CREATE (object access type constant)
  - Assert (debugging assertion macro)
  - memset (memory initialization function)

- Called from (representative examples):
  - InvokeObjectPostCreateHookArgStr
  - ObjectAccessNamespaceSearch

## Notes and Other Information
- This is part of the string-based variant of PostgreSQL's object access hook system, complementing the OID-based hooks
- The function initializes an ObjectAccessPostCreate structure with zeroed memory and sets the is_internal flag based on the parameter
- String-based hooks are particularly useful when object OIDs are not readily available or when extensions prefer to work with object names
- The is_internal flag helps extensions distinguish between user-initiated object creations and internal PostgreSQL operations
- This hook is commonly used by logical replication systems, auditing extensions, and other tools that need to track database schema changes