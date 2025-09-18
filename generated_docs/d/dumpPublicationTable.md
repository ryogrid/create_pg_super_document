# dumpPublicationTable

## Location
[src/bin/pg_dump/pg_dump.c:4697-4757](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L4697-L4757)

## Overview
Generates SQL commands to recreate publication table mappings by producing ALTER PUBLICATION statements that add specific tables to publications with optional column lists and row filters.

## Definition


## Detailed Description
This function generates the SQL DDL necessary to recreate a publication's table membership. It creates an  statement that adds a specific table to a publication, including support for column lists and WHERE clause row filters when present. The function is part of pg_dump's output generation phase and creates archive entries that will be written to the dump file.

The function handles the complete syntax for table publication including optional column specifications and row-level filtering conditions. It ensures proper parentheses around WHERE expressions since pg_get_expr doesn't supply them for simple expressions like "WHERE TRUE".

## Parameters / Member Variables
- : Archive structure for writing dump output
- : Publication relation info containing the relationship details between publication and table

## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md) - formats strings safely
  - [fmtId](../f/fmtId.md) - formats identifiers safely for SQL output
  - fmtQualifiedDumpable - formats qualified table names for dump output
  - [ArchiveEntry](../A/ArchiveEntry.md) - creates an archive entry for the dump
  - createPQExpBuffer/destroyPQExpBuffer - manages query buffers
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)/appendPQExpBufferStr - builds SQL statements
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) - [main](../m/main.md) dispatcher for dumping various object types

## Notes and Other Information
- Only operates when DUMP_COMPONENT_DEFINITION flag is set
- Skipped entirely in data-only dumps (--data-only option)
- No drop statement is generated since table drops handle this automatically
- Creates archive entries in SECTION_POST_DATA for proper restore ordering
- Supports column lists (pubrattrs) and row filters (pubrelqual) for PostgreSQL 15+
- [Publication](../P/Publication.md) table objects cannot currently have comments or security labels
- Uses the publication owner's role name for the archive entry to ensure correct restore permissions
- Adds parentheses around WHERE expressions to handle cases like "WHERE TRUE" properly