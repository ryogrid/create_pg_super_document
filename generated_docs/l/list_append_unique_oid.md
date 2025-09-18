# list_append_unique_oid

## Location
src/backend/nodes/list.c: 1380 - 1404

## Overview
Appends an OID (Object Identifier) value to a list only if it is not already present, ensuring uniqueness of OID values in the list.

## Definition
```c
List *list_append_unique_oid(List *list, Oid datum)
```

## Detailed Description
This function is a specialized variant of list_append_unique() that operates specifically on lists of OIDs (Object Identifiers). OIDs are fundamental to PostgreSQL's catalog system, uniquely identifying database objects such as tables, functions, types, and other schema elements. The function checks whether the given OID already exists in the list using list_member_oid(), and only appends the value if it's not already present.

This ensures that the resulting list contains only unique OID values, which is crucial for maintaining referential integrity and avoiding duplicate references to database objects. The function is extensively used throughout PostgreSQL's catalog management, DDL operations, and replication systems.

## Parameters / Member Variables
- `list`: The target List structure to which the OID should be appended
- `datum`: The OID value to be added to the list (only if not already present)

## Dependencies
- Functions called/Symbols referenced:
  - list_member_oid
  - lappend_oid
- Called from (representative examples):
  - hashvalidate
  - btvalidate
  - heap_truncate_find_FKs
  - ObjectsInPublicationToOids
  - ATExecAlterConstrRecurse
  - DropRole
  - LogicalRepWorkersWakeupAtCommit
  - map_sql_typecoll_to_xmlschema_types
  - forfive

## Notes and Other Information
- Returns the original list if the OID is already present
- Returns a new list with the OID appended if the value is unique
- Widely used in PostgreSQL's catalog operations, constraint management, and publication/subscription systems
- Essential for maintaining unique collections of database object references
- Part of the generic List API optimized for OID operations