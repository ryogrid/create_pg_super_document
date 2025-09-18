# list_member_oid

## Location
src/backend/nodes/list.c: 722 - 741

## Overview
Tests whether a given OID (Object Identifier) value is a member of an OID list using direct OID comparison for equality determination.

## Definition
```c
bool list_member_oid(const List *list, Oid datum)
```

## Detailed Description
The `list_member_oid` function performs membership testing on PostgreSQL's List data structure specifically for OID lists. It iterates through the list cells using the `foreach` macro and compares each cell's OID value with the target datum using direct OID comparison (`==` operator). The function includes assertions to ensure the input list is actually an OID list type and validates list invariants for debugging purposes.

This function is optimized for OID comparison and should only be used with lists that contain OID values. OIDs are PostgreSQL's internal object identifiers used to uniquely identify database objects like tables, types, functions, and other catalog entities. The function uses the `lfirst_oid` macro to extract OID values from list cells.

## Parameters / Member Variables
- `list`: A constant pointer to the List structure to search within. Must be an OID list type.
- `datum`: An OID value representing the target object identifier to search for in the list.

## Dependencies
- Functions called/Symbols referenced:
  - IsOidList - Validates that the list contains OID values
  - check_list_invariants - Performs debugging validation of list structure
  - foreach - Macro for iterating through list cells
  - lfirst_oid - Macro for accessing the OID value of a list cell

- Called from (representative examples):
  - hashvalidate - Used in hash index validation
  - CheckAttributeType - Used in catalog operations
  - heap_truncate_check_FKs - Used in foreign key constraint checking
  - ReindexIsProcessingIndex - Used in index reindexing operations
  - RelationIsVisibleExt/TypeIsVisibleExt - Used in namespace visibility checks
  - list_union_oid - Used when creating union of OID lists
  - ExecInsertIndexTuples - Used in executor for index operations
  - fireRIRrules - Used in rewrite rule processing
  - has_privs_of_role - Used in access control checking

## Notes and Other Information
- The function uses direct OID value comparison (typically 32-bit unsigned integers)
- Only suitable for OID lists; will assert if used with other list types
- Returns `true` if the OID is found, `false` otherwise
- Part of PostgreSQL's generic List API located in src/backend/nodes/list.c
- Extensively used throughout PostgreSQL for checking membership of database object identifiers
- Common use cases include checking if tables/indexes are in processing lists, namespace membership, role membership, and constraint validation
- Type-safe alternative to generic list membership functions when working with PostgreSQL object identifiers