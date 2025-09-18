# getTableDataFKConstraints

## Location
[src/bin/pg_dump/pg_dump.c:3014-3054](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L3014-L3054)

## Overview
Adds dump-order dependencies reflecting foreign key constraints to ensure referenced tables are restored before referencing tables in data-only dumps.

## Definition


## Detailed Description
This function is specifically designed for data-only dump scenarios where foreign key constraints need to be handled differently than in schema+data dumps. It iterates through all dumpable objects, identifies foreign key constraints, and creates dependencies between table data objects such that referenced tables are dumped before tables that reference them. This ordering prevents foreign key constraint violations during data restoration. The function only processes constraints where both the referencing and referenced tables have data objects scheduled for dumping.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [getDumpableObjects](getDumpableObjects.md)
  - [findTableByOid](../f/findTableByOid.md)
  - [addObjectDependency](../a/addObjectDependency.md)
  - free
- Types referenced:
  - DumpableObject
  - [ConstraintInfo](../C/ConstraintInfo.md)
  - [TableInfo](../T/TableInfo.md)
  - DO_FK_CONSTRAINT
- Called from:
  - [main](../m/main.md)

## Notes and Other Information
- Only executed in data-only dumps (not in schema+data dumps)
- In schema+data dumps, FK constraints are handled by creating them after data is loaded
- Handles circular references detection during the dependency sorting step
- May encounter impossible ordering situations with self-references or circular dependencies
- Critical for maintaining referential integrity during data-only restores
- Ensures that parent tables are populated before child tables in foreign key relationships
- Only creates dependencies when both tables involved in the constraint are being dumped