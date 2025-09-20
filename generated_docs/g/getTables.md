# getTables

## Location
[src/bin/pg_dump/pg_dump.c:6806-7251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L6806-L7251)

## Overview
The getTables function retrieves all tables and table-like objects from the PostgreSQL system catalogs and returns them in a TableInfo structure array for use by pg_dump, implementing sophisticated filtering and locking mechanisms.

## Definition

```c
TableInfo *
getTables(Archive *fout, int *numTables)
```
## Detailed Description
This function is one of the most comprehensive catalog reading functions in pg_dump. It constructs and executes a complex version-dependent SQL query to retrieve all table-like objects including regular tables, sequences, views, materialized views, foreign tables, partitioned tables, and composite types from pg_class and related system catalogs.

The function performs several critical operations: it collects comprehensive metadata for each table including relkind, namespace, owner, constraints, indexes, rules, pages, tablespaces, replication identity, row security, frozen transaction IDs, and access control information. It handles version-specific features like access methods (PostgreSQL 9.6+), identity sequences, partitioning (PostgreSQL 10+), and the removal of WITH OIDS (PostgreSQL 12+).

A key feature is its table locking mechanism that acquires ACCESS SHARE locks on dumpable tables in batches to prevent concurrent schema modifications during the dump process. The function also implements sophisticated dependency tracking for sequences and their owning tables, and handles toast table relationships while avoiding issues with partitioned table toast OIDs in certain PostgreSQL versions.

## Parameters / Member Variables
- : Archive structure containing connection information and dump configuration options
- : Pointer to integer that will be set to the number of tables found

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - pg_malloc0
  - [PQfnumber](../P/PQfnumber.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - [appendStringLiteralConn](../a/appendStringLiteralConn.md)
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [findNamespace](../f/findNamespace.md)
  - [getRoleName](getRoleName.md)
  - atoi
  - strcmp
  - [PQgetisnull](../P/PQgetisnull.md)
  - [selectDumpableTable](../s/selectDumpableTable.md)
  - fmtQualifiedDumpable
  - [GetConnection](../G/GetConnection.md)
  - [PQclear](../P/PQclear.md)
  - destroyPQExpBuffer
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Retrieves all relkinds including relations, sequences, views, composite types, materialized views, foreign tables, and partitioned tables
- Implements version-specific SQL queries to handle PostgreSQL evolution (access methods, identity sequences, partitioning, WITH OIDS removal)
- Uses batch locking mechanism to acquire ACCESS SHARE locks on dumpable tables to prevent schema changes during dump
- Handles complex join relationships with pg_depend for sequence ownership tracking and pg_tablespace for tablespace information
- Manages toast table relationships while avoiding version-specific issues with partitioned table toast OIDs
- Implements sophisticated filtering logic to determine which tables are "interesting" for dump purposes
- Supports lock timeout configuration to avoid indefinite waiting for table locks
- Uses DO_TABLE object type identifier for dump object classification
- Preserves comprehensive metadata including frozen XIDs, multixact IDs, replication identity, row security settings
- Handles foreign table server dependencies and access method information for modern PostgreSQL versions