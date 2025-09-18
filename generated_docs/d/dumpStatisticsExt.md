# dumpStatisticsExt

## Location
[src/bin/pg_dump/pg_dump.c:17160-17236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L17160-L17236)

## Overview
Writes out an extended statistics object to the dump archive, including the CREATE STATISTICS statement and optional statistics target settings.

## Definition


## Detailed Description
The  function handles dumping of PostgreSQL extended statistics objects (created with CREATE STATISTICS). Extended statistics provide improved query planning by collecting cross-column statistics like n-distinct counts, functional dependencies, and most common value lists across multiple columns.

The function performs the following operations:
1. **Definition Retrieval**: Uses  to get the complete CREATE STATISTICS statement from the database catalog
2. **Statistics Target**: Adds ALTER STATISTICS SET STATISTICS command if a custom statistics target is set (non-default value)
3. **Drop Statement**: Generates corresponding DROP STATISTICS command for cleanup
4. **Archive Creation**: Creates an archive entry with both create and drop statements
5. **Comment Handling**: Dumps any associated comments for the statistics object

The function ensures that extended statistics objects are properly recreated during database restoration with all their configuration preserved.

## Parameters / Member Variables
- : Archive pointer containing dump options and output context
- : StatsExtInfo structure containing:
  - Statistics object metadata and identifiers
  - Custom statistics target value (if any)
  - Owner and namespace information
  - Dump flags for controlling what components to include

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - [fmtId](../f/fmtId.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - fmtQualifiedDumpable
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - destroyPQExpBuffer
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Skips processing in data-only dump mode since extended statistics are schema objects
- Uses  system function to retrieve the exact CREATE STATISTICS definition
- Only generates ALTER STATISTICS SET STATISTICS when stattarget >= 0 (custom target set)
- Extended statistics objects are dumped in SECTION_POST_DATA to ensure tables and columns exist first
- Part of PostgreSQL's advanced query optimization features for multi-column statistics
- Supports dumping comments associated with the statistics object if DUMP_COMPONENT_COMMENT flag is set