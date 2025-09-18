# getPublications

## Location
src/bin/pg_dump/pg_dump.c: 4235 - 4338

## Overview
Retrieves information about all logical replication publications from the PostgreSQL system catalogs and creates PublicationInfo objects for them.

## Definition


## Detailed Description
The `getPublications` function queries the `pg_publication` system catalog to gather information about all publications in the database. Publications are a key component of PostgreSQL's logical replication feature, defining which tables and what types of changes (INSERT, UPDATE, DELETE, TRUNCATE) should be replicated.

The function handles different PostgreSQL versions gracefully:
- PostgreSQL 13.0+: Full support including `pubviaroot` (publish_via_partition_root)
- PostgreSQL 11.0+: Support for truncate operations
- PostgreSQL 10.0+: Basic publication support

For each publication found, it creates a PublicationInfo structure containing all the publication attributes and marks it as dumpable based on the current dump options.

## Parameters / Member Variables
- `fout`: Archive pointer containing dump options and database connection
- `numPublications`: Output parameter - pointer to int that will receive the count of publications found

## Dependencies
- Functions called/Symbols referenced:
  - `DumpOptions`, `PublicationInfo` (data structures)
  - `createPQExpBuffer`, `appendPQExpBufferStr` (query building)
  - `[ExecuteSqlQuery](../E/ExecuteSqlQuery.md)` (SQL execution)
  - `[PQfnumber](../P/PQfnumber.md)`, `PQgetvalue`, `PQntuples` (result processing)
  - `pg_malloc`, `pg_strdup` (memory management)
  - `[AssignDumpId](../A/AssignDumpId.md)` (dump object ID assignment)
  - `[getRoleName](getRoleName.md)` (owner name resolution)
  - `[selectDumpableObject](../s/selectDumpableObject.md)` (dumpability determination)
  - `atooid` (OID conversion)
- Called from (representative examples):
  - `[getSchemaData](getSchemaData.md)` (main schema information gathering)

## Notes and Other Information
- Returns NULL and sets *numPublications to 0 if publications are disabled via `--no-publications` option
- Only available for PostgreSQL 10.0 and later (returns NULL for older versions)
- Publications are part of PostgreSQL's built-in logical replication infrastructure
- Each PublicationInfo contains boolean flags for supported DML operations (insert, update, delete, truncate)
- The `puballtables` flag indicates whether the publication includes all tables in the database
- Memory allocated for the returned array should be managed by the caller
- Part of the logical replication backup and restore functionality in pg_dump