# dumpTableAttach

## Location
src/bin/pg_dump/pg_dump.c: 16808 - 16875

## Overview
Generates SQL commands to attach a child partition to its parent partitioned table using the ATTACH PARTITION statement.

## Definition


## Detailed Description
This function creates ALTER TABLE ATTACH PARTITION commands for PostgreSQL table partitioning. It's specifically designed to handle partitioned tables that are restored separately from their parents. The function uses a prepared statement to efficiently retrieve partition boundary expressions and then generates the appropriate ATTACH PARTITION command.

The approach of using ATTACH PARTITION instead of CREATE TABLE ... PARTITION OF is important for several reasons:
- Preserves any discrepancies in column layout between parent and child
- Allows assigning different tablespaces to partitions
- Enables restoration of partitions independently from their parent tables
- Creates separate archive entries for better restore flexibility

The function handles the complete workflow: preparing the query (if not already done), retrieving partition boundaries, and generating the final ATTACH PARTITION command.

## Parameters / Member Variables
- : Archive context containing dump configuration and prepared statement tracking
- : Structure containing partition relationship information including parent table, partition table, and dump object metadata

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - appendPQExpBufferStr
  - ExecuteSqlStatement
  - printfPQExpBuffer
  - ExecuteSqlQueryForSingleRow
  - PQgetvalue
  - appendPQExpBuffer
  - fmtQualifiedDumpable
  - ArchiveEntry
  - PQclear
  - destroyPQExpBuffer
- Types referenced:
  - Archive
  - TableAttachInfo
  - DumpOptions
  - PQExpBuffer
  - PGresult
- Called from:
  - dumpDumpableObject

## Notes and Other Information
- Uses prepared statements for efficiency when processing multiple partition attachments
- Skipped entirely in data-only dump mode since it's a schema operation
- Does not generate DROP statements as partition detachment is handled by table drops
- Sets owner field to ensure commands run with correct privileges during restore
- Creates separate ArchiveEntry for independent restoration control
- Uses pg_get_expr() to retrieve the exact partition boundary expression from system catalogs