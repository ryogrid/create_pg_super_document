# getConstraints

## Location
[src/bin/pg_dump/pg_dump.c:7822-7986](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L7822-L7986)

## Overview
Retrieves information about foreign key constraints on dumpable tables and creates corresponding ConstraintInfo entries for proper dependency handling during pg_dump operations.

## Definition

```c
void
getConstraints(Archive *fout, TableInfo tblinfo[], int numTables)
```
## Detailed Description
The getConstraints function focuses specifically on foreign key constraints, as other constraint types are handled elsewhere in the pg_dump process (unique/primary key constraints are managed with indexes, and check constraints are processed in getTableAttrs). The function constructs an optimized SQL query against pg_constraint using an OID array to limit results to tables of interest and having appropriate locks. It handles version-specific features like conindid column availability (PostgreSQL 11+) and conparentid filtering for inherited constraints. For each foreign key constraint found, it creates a ConstraintInfo structure with complete metadata and establishes proper dependencies. Special handling is implemented for foreign keys referencing partitioned tables, where the constraint must depend on partition index attach objects to ensure correct restoration order during database recovery.

## Parameters / Member Variables
- : Archive pointer containing dump configuration and database connection information  
- : Array of TableInfo structures representing tables to be dumped
- : Number of entries in the tblinfo array

## Dependencies
- Functions called/Symbols referenced:
  - [TableInfo](../T/TableInfo.md) (structure type)
  - [ConstraintInfo](../C/ConstraintInfo.md) (structure type)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - DUMP_COMPONENT_DEFINITION (flag constant)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - PGRES_TUPLES_OK (constant)
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - DO_FK_CONSTRAINT (enum value)
  - [AssignDumpId](../A/AssignDumpId.md)
  - [findTableByOid](../f/findTableByOid.md)
  - [IndxInfo](../I/IndxInfo.md) (structure type)
  - [addConstrChildIdxDeps](../a/addConstrChildIdxDeps.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)
  - [SubRelInfo](../S/SubRelInfo.md) (referenced in header)

## Notes and Other Information
- Only processes foreign key constraints (contype = 'f'); other constraint types are handled by different functions
- Includes tables with triggers or partitioned tables, as partitioned tables can have foreign keys without triggers
- Builds an OID array of target tables to create an efficient single-query approach for constraint retrieval
- Handles PostgreSQL version differences with conditional SQL for conindid and conparentid columns
- Creates dependency relationships for foreign keys pointing to partitioned tables to ensure proper index attachment ordering
- The function assumes tblinfo array is sorted by OID for efficient table lookup during constraint processing
- All created ConstraintInfo objects are marked as separate dump objects with proper namespace inheritance
- Memory allocation for ConstraintInfo array is based on the actual number of foreign key constraints found