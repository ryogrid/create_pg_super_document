# dumpSearchPath

## Location
[src/bin/pg_dump/pg_dump.c:3614-3675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L3614-L3675)

## Overview
The  function records the active search_path from the source database to ensure schemas are resolved correctly during restoration.

## Definition

```c
static void
dumpSearchPath(Archive *AH)
```
## Detailed Description
The  function captures the current database's search path and stores it as a restoration command in the dump archive. Rather than using the search_path GUC directly, it queries current_schemas(false) to get the actual resolved schema names, avoiding wildcards like '' that might not be valid during restoration. The function constructs a set_config() call instead of a simple SET command for better backwards compatibility, especially when dealing with empty search paths. The resolved search path is also stored in the Archive structure for use in plain text dumps.

## Parameters / Member Variables
- : Pointer to the Archive structure where the search path command will be stored and archived

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md) (executes current_schemas() query)
  - [parsePGArray](../p/parsePGArray.md) (parses the PostgreSQL array result into C array)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)/appendPQExpBufferStr/destroyPQExpBuffer (string buffer management)
  - [fmtId](../f/fmtId.md) (properly quotes schema names as SQL identifiers)
  - appendStringLiteralAH (safely quotes the search path as SQL literal)
  - pg_log_info (logs the search path being saved)
  - [createDumpId](../c/createDumpId.md) (generates unique dump ID)
  - [ArchiveEntry](../A/ArchiveEntry.md) (creates archive entry with SQL command)
  - ARCHIVE_OPTS/SECTION_PRE_DATA (archive configuration macros)
  - [pg_strdup](../p/pg_strdup.md) (duplicates string for storage in AH->searchpath)
- Called from (representative examples):
  - [main](../m/main.md) (pg_dump main function)
  - fmtQualifiedDumpable

## Notes and Other Information
- Uses current_schemas(false) instead of search_path GUC to avoid wildcard expansion issues during restoration
- Employs set_config() rather than SET command for better handling of edge cases like empty search paths
- The resolved search path is stored in both the archive entry and AH->searchpath for different dump formats
- Critical for ensuring schema-qualified object references resolve correctly during restoration
- Placed in SECTION_PRE_DATA to ensure search path is set before other database objects are processed