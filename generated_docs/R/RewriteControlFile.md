# RewriteControlFile

## Location
[src/bin/pg_resetwal/pg_resetwal.c:861-906](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_resetwal/pg_resetwal.c#L861-L906)

## Overview
RewriteControlFile modifies the PostgreSQL control file to reflect the new state after a WAL reset operation, setting up the database for a fresh start with an empty transaction log.

## Definition
```c
static void RewriteControlFile(void)
```

## Detailed Description
This static function is a critical component of the pg_resetwal utility that performs the actual modification of the control file (pg_control). After all validations and user confirmations are complete, this function updates the global ControlFile structure with new values and writes it to disk.

The function performs several key operations:
1. Sets the redo point to the beginning of the new WAL segment
2. Updates the checkpoint time to the current time
3. Sets the database state to cleanly shutdown (DB_SHUTDOWNED)
4. Resets recovery-related fields to indicate no recovery is needed
5. Forces conservative default values for various configuration parameters
6. Sets WAL level to minimal since the database is starting fresh

## Parameters / Member Variables
This function takes no parameters and operates on the global ControlFile structure.

## Dependencies
- Functions called/Symbols referenced:
  - XLogSegNoOffsetToRecPtr (converts segment number and offset to record pointer)
  - time (standard C library function to get current time)
  - [update_controlfile](../u/update_controlfile.md) (writes the control file to disk)
  - SizeOfXLogLongPHD (constant for WAL page header size)
  - DB_SHUTDOWNED (database state constant)
  - WAL_LEVEL_MINIMAL (WAL level constant)

- Called from:
  - [main](../m/main.md) (in pg_resetwal.c at line 494)

## Notes and Other Information
- This is a static function local to pg_resetwal.c
- The function forces conservative defaults for connection limits and WAL settings
- Sets wal_level to minimal, which will be adjusted by the postmaster at startup if needed
- The comment indicates that many of the forced values don't matter much since they'll be reset at startup
- This function performs the actual "point of no return" operation that modifies the control file
- The update_controlfile call with the flush parameter set to true ensures data is written to disk immediately
- After this function completes, the database will have a new, clean control file ready for startup