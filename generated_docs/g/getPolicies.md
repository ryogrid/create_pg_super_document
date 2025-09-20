# getPolicies

## Location
[src/bin/pg_dump/pg_dump.c:3950-4116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L3950-L4116)

## Overview
Retrieves information about all Row-Level Security (RLS) policies on dumpable tables from the PostgreSQL system catalogs and creates PolicyInfo objects for them.

## Definition

```c
void
getPolicies(Archive *fout, TableInfo tblinfo[], int numTables)
```
## Detailed Description
The `getPolicies` function is responsible for gathering all RLS policy information during a pg_dump operation. It performs a two-phase process:

1. **RLS Status Detection**: First identifies tables that have Row-Level Security enabled and creates special PolicyInfo objects (with NULL policy names) to represent the "ENABLE ROW LEVEL SECURITY" state.

2. **Policy Retrieval**: Queries the `pg_policy` system catalog to retrieve detailed information about actual security policies, including policy names, commands (SELECT/INSERT/UPDATE/DELETE), roles, qualifiers, and WITH CHECK expressions.

The function handles version differences in PostgreSQL (9.5+ for basic RLS, 10.0+ for permissive policies, 13.0+ for publish_via_root) and only processes tables marked for policy dumping. It constructs SQL queries dynamically based on the PostgreSQL version to ensure compatibility.

## Parameters / Member Variables
- `fout`: Archive pointer containing dump options and database connection information
- `tblinfo[]`: Array of TableInfo structures representing all tables in the database
- `numTables`: Number of tables in the tblinfo array

## Dependencies
- Functions called/Symbols referenced:
  - [TableInfo](../T/TableInfo.md), `PolicyInfo` (data structures)
  - `createPQExpBuffer`, `appendPQExpBuffer` series (query building)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) (SQL execution)
  - `pg_malloc`, `pg_strdup` (memory management)
  - [AssignDumpId](../A/AssignDumpId.md) (dump object ID assignment)
  - [findTableByOid](../f/findTableByOid.md) (table lookup)
  - `pg_log_info` (logging)
  - `DUMP_COMPONENT_POLICY` (component flags)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md) (main schema information gathering)

## Notes and Other Information
- Only available for PostgreSQL 9.5 and later (returns early for older versions)
- Creates two types of PolicyInfo objects: RLS-enabled markers (polname=NULL) and actual policies
- Handles version-specific features like permissive policies (10.0+) and publish_via_root (13.0+)
- Uses array-based SQL queries with unnest() for efficient bulk processing of multiple tables
- Policy expressions are retrieved using `pg_get_expr()` to reconstruct the original SQL text
- Part of the comprehensive schema information gathering phase of pg_dump operations