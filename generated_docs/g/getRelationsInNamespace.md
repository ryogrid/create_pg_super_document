# getRelationsInNamespace

## Location
[src/backend/catalog/aclchk.c:938-975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L938-L975)

## Overview
Returns a list of relation OIDs in a given namespace, filtered by a specific relation kind (e.g., tables, sequences, views).

## Definition

```c
static List *
getRelationsInNamespace(Oid namespaceId, char relkind)
```
## Detailed Description
This function performs a catalog scan on the pg_class system table to find all relations within a specified namespace that match a particular relation kind. It uses a two-key scan strategy: first filtering by namespace ID and then by relation kind. The function opens the pg_class catalog with an AccessShareLock, performs a sequential scan using the constructed scan keys, and builds a list of OIDs for all matching relations. This is an efficient way to enumerate specific types of objects within a schema without requiring individual lookups.

## Parameters / Member Variables
- : The OID of the namespace (schema) to search within
- : A character representing the type of relation to find (e.g., RELKIND_RELATION for tables, RELKIND_SEQUENCE for sequences, RELKIND_VIEW for views)

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [CharGetDatum](../C/CharGetDatum.md)
  - [table_open](../t/table_open.md)
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)
  - [heap_getnext](../h/heap_getnext.md)
  - [lappend_oid](../l/lappend_oid.md)
  - [table_endscan](../t/table_endscan.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [objectsInSchemaToOids](../o/objectsInSchemaToOids.md)
  - InternalDefaultACL

## Notes and Other Information
- The function is static and used internally within aclchk.c for ACL and privilege management operations
- Uses a composite scan key with both namespace and relation kind filters for efficiency
- The relkind parameter corresponds to the relkind column in pg_class, which distinguishes between different types of relations
- Common relkind values include 'r' (ordinary table), 'S' (sequence), 'v' (view), 'm' (materialized view), 'f' (foreign table), 'p' (partitioned table)
- The function acquires AccessShareLock on pg_class, which allows concurrent reads but prevents schema modifications during the scan
- Returns an empty list (NIL) if no matching relations are found