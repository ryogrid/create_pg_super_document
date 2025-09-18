# _printTableAccessMethodNoStorage

## Location
src/bin/pg_dump/pg_backup_archiver.c: 3616 - 3663

## Overview
Sets the table access method for partitioned tables that have no storage using ALTER TABLE SET ACCESS METHOD during PostgreSQL database restore operations.

## Definition
```c
static void _printTableAccessMethodNoStorage(ArchiveHandle *AH, TocEntry *te)
```

## Detailed Description
The `_printTableAccessMethodNoStorage` function handles a specific case in PostgreSQL table access method restoration: setting the access method for partitioned tables. Unlike regular tables, partitioned tables themselves have no storage (they're metadata-only containers for their partitions), so they require a different approach using ALTER TABLE SET ACCESS METHOD rather than the default_table_access_method setting.

This function specifically handles partitioned tables (RELKIND_PARTITIONED_TABLE) that have a specified table access method. It generates and executes an ALTER TABLE statement to set the access method for the partitioned table after it has been created.

The function includes safety checks and respects restore options:
- Honors the `--no-table-access-method` restore option
- Only processes entries that have a table access method specified
- Asserts that the target is indeed a partitioned table
- Works with both direct database connections and script output modes

## Parameters / Member Variables
- `AH`: Archive handle containing connection info, restore options, and database connection
- `te`: TOC entry representing the partitioned table, containing namespace, name, and table access method information

## Dependencies
- Functions called/Symbols referenced:
  - `[fmtQualifiedId](../f/fmtQualifiedId.md)` - Formats schema-qualified table names with proper quoting
  - `[fmtId](../f/fmtId.md)` - Formats PostgreSQL identifiers with proper quoting  
  - `[RestoringToDB](../R/RestoringToDB.md)` - Checks if restoring directly to database vs script output
  - `[PQexec](../P/PQexec.md)` - Executes SQL command on database connection
  - `[warn_or_exit_horribly](../w/warn_or_exit_horribly.md)` - Error handling for restore operations
  - `[ahprintf](../a/ahprintf.md)` - Outputs formatted text to archive handle
- Data types referenced:
  - `[TocEntry](../T/TocEntry.md)` - Structure representing a database object in the restore archive
  - `[RestoreOptions](../R/RestoreOptions.md)` - Structure containing restore configuration options
- Called from (representative examples):
  - `[_printTocEntry](_printTocEntry.md)` - TOC entry output function during the restore process

## Notes and Other Information
- This is a static function, only accessible within pg_backup_archiver.c
- Specifically designed for partitioned tables (RELKIND_PARTITIONED_TABLE) which have no physical storage themselves
- Uses ALTER TABLE SET ACCESS METHOD syntax rather than default_table_access_method setting
- Part of PostgreSQL's pg_dump/pg_restore infrastructure for handling pluggable table access methods
- The function includes an assertion to ensure it's only called for partitioned tables
- Respects the `noTableAm` restore option to allow users to disable table access method handling
- Essential for properly restoring partitioned table configurations in PostgreSQL 12+ where table access methods are supported
- Uses libpq functions (PQexec, PQclear) for database communication when restoring directly to a database