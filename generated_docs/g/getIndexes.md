# getIndexes

## Location
[src/bin/pg_dump/pg_dump.c:7433-7742](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L7433-L7742)

## Overview
Retrieves comprehensive information about all indexes on dumpable tables and creates corresponding DumpableObject entries for use during pg_dump operations.

## Definition

```c
void
getIndexes(Archive *fout, TableInfo tblinfo[], int numTables)
```
## Detailed Description
The getIndexes function performs a sophisticated query against PostgreSQL system catalogs to gather complete index information for all tables marked for dumping. It constructs a single optimized SQL query that retrieves index definitions, statistics, constraint relationships, and metadata from multiple system tables including pg_index, pg_class, pg_constraint, and pg_inherits. The function handles version-specific features like replica identity indexes (9.4+), partitioned indexes (11.0+), and NULLS NOT DISTINCT support (15.0+). For each index found, it creates IndxInfo structures and populates them with detailed metadata. Additionally, when indexes are associated with constraints (primary key, unique, or exclusion), it creates corresponding ConstraintInfo entries, establishing proper dependency relationships for correct dump ordering.

## Parameters / Member Variables
- : Archive pointer containing dump configuration and database connection information
- : Array of TableInfo structures representing tables to be dumped
- : Number of entries in the tblinfo array

## Dependencies
- Functions called/Symbols referenced:
  - [TableInfo](../T/TableInfo.md) (structure type)
  - [IndxInfo](../I/IndxInfo.md) (structure type)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - PGRES_TUPLES_OK (constant)
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - DO_INDEX (enum value)
  - [AssignDumpId](../A/AssignDumpId.md)
  - [parseOidArray](../p/parseOidArray.md)
  - [SimplePtrList](../S/SimplePtrList.md) (structure type)
  - [ConstraintInfo](../C/ConstraintInfo.md) (structure type)
  - DO_CONSTRAINT (enum value)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)
  - [SubRelInfo](../S/SubRelInfo.md) (referenced in header)

## Notes and Other Information
- Only processes tables that have the hasindex flag set and are marked as interesting
- Builds an OID array of target tables to optimize the single SQL query approach
- Handles PostgreSQL version differences with conditional SQL generation
- Creates constraint entries for primary key, unique, and exclusion constraint indexes
- Supports partitioned index inheritance relationships (PostgreSQL 11+)
- Retrieves index statistics columns and values for performance analysis
- The function assumes tblinfo array is sorted by OID for efficient table lookup
- Index data is stored in TableInfo structures rather than returned directly
- Memory management includes proper allocation for IndxInfo arrays and string fields