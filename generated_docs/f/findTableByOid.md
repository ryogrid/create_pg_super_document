# findTableByOid

## Location
[src/bin/pg_dump/common.c:852-869](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L852-L869)

## Overview
Finds and returns the DumpableObject for a table with the specified OID, returning NULL if not found.

## Definition

```c
TableInfo *
findTableByOid(Oid oid)
```
## Detailed Description
This function serves as a specialized lookup utility for finding TableInfo objects by their database OID. It constructs a CatalogId structure using the provided OID and the RelationRelationId (which identifies the pg_class system catalog), then uses the generic findObjectByCatalogId function to locate the corresponding DumpableObject. The function includes an assertion to verify that any found object is indeed a table (DO_TABLE type) before casting and returning it as a TableInfo pointer.

This function is essential for resolving table references throughout pg_dump's operation, allowing various parts of the system to locate table objects when they have only the OID available. It's commonly used when processing foreign key constraints, inheritance relationships, and other inter-table dependencies.

## Parameters / Member Variables
- : The database OID of the table to find

## Dependencies
- Functions called/Symbols referenced:
  - [findObjectByCatalogId](findObjectByCatalogId.md) (generic object lookup by catalog ID)
  - [CatalogId](../C/CatalogId.md) (structure for identifying catalog objects)
  - DumpableObject (base structure type for dumpable objects)
  - DO_TABLE (enum value for table object type)
  - [TableInfo](../T/TableInfo.md) (structure type for table information)
- Called from (representative examples):
  - [flagInhTables](flagInhTables.md) (in src/bin/pg_dump/common.c:320,332)
  - [selectDumpableType](../s/selectDumpableType.md) (in src/bin/pg_dump/pg_dump.c:1915)
  - [getTableDataFKConstraints](../g/getTableDataFKConstraints.md) (in src/bin/pg_dump/pg_dump.c:3033)
  - [getConstraints](../g/getConstraints.md) (in src/bin/pg_dump/pg_dump.c:7946)
  - [processExtensionTables](../p/processExtensionTables.md) (in src/bin/pg_dump/pg_dump.c:18436,18521,18522)

## Notes and Other Information
- Returns NULL if no table with the specified OID is found in the dump object registry
- Uses RelationRelationId as the tableoid component of the CatalogId, indicating this searches within the pg_class system catalog
- Includes a debug assertion that verifies the found object is actually a table type before returning it
- The function assumes that if an object is found with the given OID, it should be a table - the assertion will fail if this assumption is violated
- Part of a family of similar lookup functions (like findIndexByOid) that provide type-safe access to specific kinds of database objects
- Critical for maintaining referential integrity and proper dependency tracking in pg_dump's object model