# movedb_failure_callback

## Location
[src/backend/commands/dbcommands.c:2286-2302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L2286-L2302)

## Overview
movedb_failure_callback is an error cleanup function that removes partially copied database files when a movedb operation fails, ensuring the target directory is cleaned up.

## Definition
```c
static void movedb_failure_callback(int code, Datum arg)
```

## Detailed Description
movedb_failure_callback serves as an error recovery mechanism for the movedb operation. When registered with PostgreSQL's error cleanup system via PG_ENSURE_ERROR_CLEANUP, this callback automatically executes if an error occurs during database file copying. It removes any files that were successfully copied to the target directory before the failure, preventing orphaned files from remaining in the destination tablespace. This ensures that failed movedb operations leave the system in a clean state.

## Parameters / Member Variables
- `code`: Error code (standard parameter for PostgreSQL error callbacks, not used in this function)
- `arg`: Datum containing a pointer to movedb_failure_params structure with destination database and tablespace information

## Dependencies
- Functions called/Symbols referenced:
  - movedb_failure_params: Structure containing destination database and tablespace OIDs
  - [GetDatabasePath](../G/GetDatabasePath.md): Constructs the destination directory path
  - rmtree: Recursively removes the destination directory and all its contents
- Called from (representative examples):
  - [movedb](movedb.md): Registered as error cleanup callback during database move operations

## Notes and Other Information
- Used exclusively with PostgreSQL's PG_ENSURE_ERROR_CLEANUP mechanism
- Provides best-effort cleanup - operates during error conditions so additional failures are handled gracefully
- Only removes files from the destination directory, leaving source files untouched
- Critical for maintaining file system consistency when database moves fail partway through
- The callback parameter structure (movedb_failure_params) contains dest_dboid and dest_tsoid fields needed to reconstruct the target path