# recovery_create_dbdir

## Location
src/backend/commands/dbcommands.c: 3241 - 3269

## Overview
A recovery-specific function that creates missing tablespace directories during WAL recovery when they are needed for database creation operations.

## Definition


## Detailed Description
This function handles a specific recovery scenario where PostgreSQL needs to create a database but the required tablespace directory is missing. During recovery, if a tablespace was removed before the server stopped but there are WAL records for database creation in that tablespace, this function creates the necessary directory structure. It includes safety checks to ensure directories are only created in appropriate locations and implements different behavior based on whether recovery consistency has been reached. The function creates actual directories under pg_tblspc rather than restoring symbolic links.

## Parameters / Member Variables
- : The filesystem path of the directory to create
- : Boolean flag indicating whether the directory must be within pg_tblspc/ (true) or can be elsewhere (false)

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro)
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (recovery state check)
  - [stat](../s/stat.md) (filesystem status check)
  - strstr (string search)
  - elog (error/log reporting)
  - ereport (enhanced error reporting)
  - [pg_mkdir_p](../p/pg_mkdir_p.md) (recursive directory creation)
  - PANIC (error level constant)
  - DEBUG1/WARNING (logging level constants)
- Called from (representative examples):
  - [dbase_redo](../d/dbase_redo.md) (database WAL record replay - multiple call sites)

## Notes and Other Information
- Static function only used during WAL recovery operations
- Creates real directories instead of symbolic links for simplicity during recovery
- Different logging behavior before and after reaching recovery consistency
- Includes safety checks to prevent creation of directories outside pg_tblspc when only_tblspc is true
- Part of PostgreSQL's crash recovery mechanism for handling missing tablespace directories
- Uses pg_mkdir_p for recursive directory creation with proper permissions
- Critical for ensuring database creation can proceed during recovery even when tablespaces are missing