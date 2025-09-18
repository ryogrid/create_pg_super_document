# _selectOutputSchema

## Location
src/bin/pg_dump/pg_backup_archiver.c: 3455 - 3505

## Overview
Sets the search_path in the target database to select a specific schema as the current schema during PostgreSQL database restore operations.

## Definition
```c
static void _selectOutputSchema(ArchiveHandle *AH, const char *schemaName)
```

## Detailed Description
The `_selectOutputSchema` function manages schema context during database restore operations by setting the PostgreSQL search_path. This ensures that objects are created in or referenced from the correct schema during the restore process.

The function intelligently handles several scenarios:
- Respects existing SEARCHPATH TOC entries from newer archives
- Avoids redundant schema switches when already in the target schema
- Automatically includes pg_catalog in the search path for non-catalog schemas
- Handles both direct database connections and script output modes

When restoring to a live database connection, it executes the SET search_path command directly. When generating a script, it outputs the command to the script file.

## Parameters / Member Variables
- `AH`: Archive handle containing connection info, restore options, and current state
- `schemaName`: Name of the schema to select as current schema (NULL/empty for no change)

## Dependencies
- Functions called/Symbols referenced:
  - `fmtId` - Formats PostgreSQL identifiers with proper quoting
  - `RestoringToDB` - Checks if restoring directly to database vs script
  - `PQexec` - Executes SQL command on database connection
  - `warn_or_exit_horribly` - Error handling for restore operations
  - `ahprintf` - Outputs formatted text to archive handle
- Called from (representative examples):
  - `RestoreArchive` - Main restore orchestration function
  - `restore_toc_entry` - Individual object restore function
  - `_printTocEntry` - TOC entry output function

## Notes and Other Information
- This is a static function, only accessible within pg_backup_archiver.c
- Maintains the current schema state in `AH->currSchema` to avoid redundant operations
- Automatically adds pg_catalog to the search path for non-catalog schemas to ensure system functions remain accessible
- Respects the archive's searchpath setting - newer archives with SEARCHPATH entries skip schema switching
- Part of PostgreSQL's pg_dump/pg_restore infrastructure for maintaining proper schema context during restore
- Uses libpq functions (PQexec, PQclear) for database communication when restoring directly to a database