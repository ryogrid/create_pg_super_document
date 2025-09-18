# _selectTablespace

## Location
src/bin/pg_dump/pg_backup_archiver.c: 3506 - 3565

## Overview
Sets the default_tablespace setting in the target database to specify where database objects should be created during PostgreSQL database restore operations.

## Definition
```c
static void _selectTablespace(ArchiveHandle *AH, const char *tablespace)
```

## Detailed Description
The `_selectTablespace` function manages tablespace context during database restore operations by setting the PostgreSQL default_tablespace parameter. This ensures that database objects (tables, indexes, etc.) are created in the correct tablespace during the restore process.

The function includes several intelligent optimizations:
- Respects the `--no-tablespaces` restore option to disable tablespace handling
- Avoids redundant tablespace switches when already using the target tablespace
- Handles both explicit tablespace names and the default tablespace (empty string)
- Works with both direct database connections and script output modes

When an empty string is provided as the tablespace name, it sets the default_tablespace to empty, which means objects will be created in the database's default tablespace.

## Parameters / Member Variables
- `AH`: Archive handle containing connection info, restore options, and current tablespace state
- `tablespace`: Name of the tablespace to set as default (NULL for no change, empty string for database default)

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
- Maintains the current tablespace state in `AH->currTablespace` to avoid redundant SET operations
- Respects the `noTablespace` restore option, allowing users to disable tablespace handling entirely
- Handles the special case of empty string tablespace name to reset to database default
- Part of PostgreSQL's pg_dump/pg_restore infrastructure for maintaining proper tablespace context during restore
- Uses libpq functions (PQexec, PQclear) for database communication when restoring directly to a database
- The function is called less frequently than schema selection, primarily during object creation rather than for every restore operation