# _selectTableAccessMethod

## Location
[src/bin/pg_dump/pg_backup_archiver.c:3566-3615](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L3566-L3615)

## Overview
Sets the default_table_access_method parameter in the target database to specify the storage engine for tables during PostgreSQL database restore operations.

## Definition
```c
static void _selectTableAccessMethod(ArchiveHandle *AH, const char *tableam)
```

## Detailed Description
The `_selectTableAccessMethod` function manages table access method context during database restore operations by setting the PostgreSQL default_table_access_method parameter. This ensures that tables are created using the correct storage engine (such as heap, columnar, or other pluggable table access methods) during the restore process.

The function includes several key features:
- Respects the `--no-table-access-method` restore option to disable table access method handling
- Avoids redundant access method switches when already using the target method  
- Handles only non-NULL table access method names (skips if NULL)
- Works with both direct database connections and script output modes

Table access methods were introduced in PostgreSQL 12 as a pluggable storage interface, allowing different storage engines for tables while maintaining SQL compatibility.

## Parameters / Member Variables
- `AH`: Archive handle containing connection info, restore options, and current table access method state
- `tableam`: Name of the table access method to set as default (NULL for no change)

## Dependencies
- Functions called/Symbols referenced:
  - [fmtId](../f/fmtId.md) - Formats PostgreSQL identifiers with proper quoting
  - [RestoringToDB](../R/RestoringToDB.md) - Checks if restoring directly to database vs script output
  - [PQexec](../P/PQexec.md) - Executes SQL command on database connection
  - [warn_or_exit_horribly](../w/warn_or_exit_horribly.md) - Error handling for restore operations
  - [ahprintf](../a/ahprintf.md) - Outputs formatted text to archive handle
- Data types referenced:
  - [RestoreOptions](../R/RestoreOptions.md) - Structure containing restore configuration options
- Called from (representative examples):
  - [_printTocEntry](../p/_printTocEntry.md) - TOC entry output function that manages object creation context

## Notes and Other Information
- This is a static function, only accessible within pg_backup_archiver.c
- Maintains the current table access method state in `AH->currTableAm` to avoid redundant SET operations
- Respects the `noTableAm` restore option, allowing users to disable table access method handling entirely
- Part of PostgreSQL's pg_dump/pg_restore infrastructure for maintaining proper storage engine context during restore
- Table access methods are a PostgreSQL 12+ feature for pluggable table storage engines
- Uses libpq functions (PQexec, PQclear) for database communication when restoring directly to a database
- Only processes non-NULL tableam parameters, skipping when no table access method is specified
- The function helps ensure that restored tables use the same storage engine as in the original database