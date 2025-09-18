# RunObjectPostAlterHook

## Location
src/backend/catalog/objectaccess.c: 92 - 114

## Overview
Executes registered object access hooks for post-alter events, allowing extensions to perform actions after a PostgreSQL database object has been modified.

## Definition
```c
void RunObjectPostAlterHook(Oid classId, Oid objectId, int subId, Oid auxiliaryId, bool is_internal)
```

## Detailed Description
This function serves as the entry point for the OAT_POST_ALTER object access hook event. It is called after a database object (such as a table, index, function, etc.) has been successfully altered in the PostgreSQL system catalogs. The function provides a standardized way for extensions to hook into the object modification process and perform additional processing, audit logging, or validation.

The function constructs an ObjectAccessPostAlter structure with the provided parameters and invokes any registered object access hooks through the global object_access_hook function pointer. This enables extensions to extend PostgreSQL's functionality by responding to object alteration events, which is particularly useful for implementing security policies, change tracking, or custom validation logic.

## Parameters / Member Variables
- `classId`: The OID of the system catalog (pg_class, pg_proc, etc.) containing the altered object
- `objectId`: The OID of the altered object within the specified catalog
- `subId`: Sub-object identifier (e.g., column number for table columns, 0 for whole objects)
- `auxiliaryId`: Secondary identifier used for catalogs that require two IDs to uniquely identify a tuple (e.g., pg_inherits, pg_db_role_setting, pg_user_mapping); should be InvalidOid elsewhere
- `is_internal`: Boolean flag indicating whether this alteration is internal to PostgreSQL operations (true) or user-initiated (false)

## Dependencies
- Functions called/Symbols referenced:
  - [ObjectAccessPostAlter](../O/ObjectAccessPostAlter.md) (struct type)
  - OAT_POST_ALTER (enum value)
  - object_access_hook (global function pointer)
  - Assert (assertion macro)
  - memset (memory initialization function)

- Called from (representative examples):
  - [ObjectAccessNamespaceSearch](../O/ObjectAccessNamespaceSearch.md)
  - InvokeObjectPostAlterHookArg

## Notes and Other Information
- The function includes an assertion to ensure object_access_hook is not NULL, though the comment suggests callers should verify this condition
- The ObjectAccessPostAlter structure is zero-initialized before setting the auxiliary_id and is_internal fields
- The auxiliary_id parameter is specifically designed for catalogs that use composite keys (two OIDs) to identify tuples
- The is_internal flag allows extensions to differentiate between user-initiated changes and internal PostgreSQL operations (e.g., constraint modifications during CLUSTER)
- This is part of PostgreSQL's extensibility framework, enabling custom change tracking, audit systems, and security policy enforcement
- Extensions can use this hook to implement row-level security updates, cache invalidation, or replication triggers