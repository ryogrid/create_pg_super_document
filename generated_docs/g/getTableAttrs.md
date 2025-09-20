# getTableAttrs

## Location
[src/bin/pg_dump/pg_dump.c:8805-9361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L8805-L9361)

## Overview
Retrieves detailed information about table attributes (columns) including names, types, defaults, constraints, and metadata for all interesting tables in a pg_dump operation.

## Definition

```c
void
getTableAttrs(Archive *fout, TableInfo *tblinfo, int numTables)
```
## Detailed Description
The  function performs comprehensive attribute collection for tables that are marked as interesting for dumping. It executes multiple carefully constructed SQL queries against the system catalogs to gather column metadata, default expressions, and CHECK constraints. The function implements version-specific logic to handle features introduced in different PostgreSQL versions (compression in 14.0+, identity columns in 10.0+, missing values in 11.0+, generated columns in 12.0+).

The function operates in three main phases: 1) Collect basic column information from pg_attribute, 2) Retrieve column default expressions from pg_attrdef, and 3) Gather CHECK constraint definitions from pg_constraint. It uses array-based queries with unnest() to efficiently batch operations while maintaining proper locking constraints.

## Parameters / Member Variables
- : Archive pointer for the pg_dump operation, containing version info and dump options
- : Array of TableInfo structures to populate with attribute information
- : Number of tables in the tblinfo array

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - appendPQExpBufferChar
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - atooid
  - pg_malloc
  - [pg_strdup](../p/pg_strdup.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [AssignDumpId](../A/AssignDumpId.md)
  - [shouldPrintColumn](../s/shouldPrintColumn.md)
  - [addObjectDependency](../a/addObjectDependency.md)
  - pg_log_info
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - ngettext
  - pg_log_error_hint
  - [exit_nicely](../e/exit_nicely.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Skips sequences and tables marked as uninteresting to optimize performance
- Implements version-dependent SQL queries for features like compression, identity, and generated columns
- Uses efficient batch queries with unnest() arrays to reduce round-trips to the database
- Handles both inline and separate default/constraint dumping based on various conditions
- Properly manages memory allocation for all attribute arrays within each TableInfo structure
- Validates column numbering and constraint counts for data integrity
- Supports inheritance-aware attribute handling through inhNotNull tracking
- Only processes defaults and constraints when not doing a data-only dump