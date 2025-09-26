# ResetUnloggedRelationsInTablespaceDir

## Location
[src/backend/storage/file/reinit.c:106-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/reinit.c#L106-L160)

## Overview
ResetUnloggedRelationsInTablespaceDir processes a single tablespace directory to reset unlogged relations, iterating through database directories within the tablespace and delegating the actual reset work to individual database processing.

## Definition
```c
static void ResetUnloggedRelationsInTablespaceDir(const char *tsdirname, int op)
```

## Detailed Description
This function serves as an intermediate layer in the unlogged relation reset process, focusing on processing a specific tablespace directory. It:

1. Opens the specified tablespace directory (e.g., "base" for default tablespace or "pg_tblspc/[oid]/PG_17_6" for user tablespaces)
2. Iterates through subdirectories, filtering for database directories (identified by numeric names representing database OIDs)
3. Reports progress for each database being processed during startup
4. Delegates the actual reset work to `ResetUnloggedRelationsInDbspaceDir` for each database

The function includes robust error handling, particularly for the case where a tablespace directory doesn't exist (ENOENT), which can occur if a previous DROP TABLESPACE operation was interrupted. Rather than failing startup, it logs the issue and continues.

## Parameters / Member Variables
- `tsdirname`: Path to the tablespace directory to process (e.g., "base", "pg_tblspc/16384/PG_17_6")
- `op`: Bitwise operation flags inherited from parent function
  - `UNLOGGED_RELATION_CLEANUP` (0x0001): Enable cleanup operations
  - `UNLOGGED_RELATION_INIT` (0x0002): Enable initialization operations

## Dependencies
- Functions called/Symbols referenced:
  - `AllocateDir`/`ReadDir`/`FreeDir`: Directory traversal operations
  - `ereport`: Error reporting for missing directories
  - `report_startup_progress`: Progress reporting during startup
  - `ResetUnloggedRelationsInDbspaceDir`: Processes individual database directories
  - `strspn`/`strlen`: String validation for numeric directory names
  - `snprintf`: Path construction

- Called from:
  - `ResetUnloggedRelations`: For each tablespace directory (lines 75, 90)

## Notes and Other Information
- This is a static function, internal to the reinit.c module
- Uses numeric directory name validation to identify database directories (OID-based)
- Gracefully handles missing tablespace directories from incomplete DROP TABLESPACE operations
- Provides detailed progress reporting showing current database path being processed
- The function distinguishes between cleanup and init operations in progress messages
- Located in src/backend/storage/file/reinit.c:106-160