# list_append_unique_oid

## Location
[src/backend/nodes/list.c:1380-1404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1380-L1404)

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
  - [list_member_oid](list_member_oid.md)
  - [lappend_oid](lappend_oid.md)
- Called from (representative examples):
  - [hashvalidate](../h/hashvalidate.md)
  - [btvalidate](../b/btvalidate.md)
  - [heap_truncate_find_FKs](../h/heap_truncate_find_FKs.md)
  - [ObjectsInPublicationToOids](../O/ObjectsInPublicationToOids.md)
  - [ATExecAlterConstrRecurse](../A/ATExecAlterConstrRecurse.md)
  - [DropRole](../D/DropRole.md)
  - [LogicalRepWorkersWakeupAtCommit](../L/LogicalRepWorkersWakeupAtCommit.md)
  - [map_sql_typecoll_to_xmlschema_types](../m/map_sql_typecoll_to_xmlschema_types.md)
  - forfive

## Notes and Other Information
- Returns the original list if the OID is already present
- Returns a new list with the OID appended if the value is unique
- Widely used in PostgreSQL's catalog operations, constraint management, and publication/subscription systems
- Essential for maintaining unique collections of database object references
- Part of the generic List API optimized for OID operations

## Simplified Source

```c
List *
list_append_unique_oid(List *list, Oid datum)
{
    // Check if OID already exists in the list
    if (list_member_oid(list, datum))
        return list;  // Return unchanged list if duplicate
    else
        return lappend_oid(list, datum);  // Append if unique
}
```