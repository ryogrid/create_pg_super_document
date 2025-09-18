# pg_current_wal_flush_lsn

## Location
src/backend/access/transam/xlogfuncs.c: 315 - 336

## Overview
Returns the current WAL flush location, which represents the position up to which WAL records have been flushed to disk storage.

## Definition
```c
Datum pg_current_wal_flush_lsn(PG_FUNCTION_ARGS)
```

## Detailed Description
This function reports the current WAL (Write-Ahead Log) flush location in the same format as functions like `pg_backup_start`. The flush location indicates the position up to which WAL records have been successfully flushed (synchronized) to persistent storage. This is different from the insert location, as records may be inserted into WAL buffers but not yet flushed to disk.

The function is primarily intended for debugging purposes and provides insight into the current state of WAL record flushing. Like other WAL control functions, it cannot be executed during recovery mode.

## Parameters / Member Variables
- This function takes no parameters (uses `PG_FUNCTION_ARGS` macro for PostgreSQL function interface)

## Dependencies
- Functions called/Symbols referenced:
  - `[RecoveryInProgress](../R/RecoveryInProgress.md)()` - Checks if database recovery is currently active
  - `[GetFlushRecPtr](../G/GetFlushRecPtr.md)()` - Retrieves the current WAL flush record pointer (with NULL parameter)
  - `PG_RETURN_LSN` - Macro to return LSN value to PostgreSQL function caller
- Called from (representative examples):
  - No direct callers found in the codebase (SQL-callable function)

## Notes and Other Information
- This function is mostly for debugging purposes as noted in the source code comments
- The function will raise an error if called during recovery, with error code `ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE`
- Returns the LSN in the standard PostgreSQL LSN format (e.g., '0/1667D48')
- The flush location is typically behind or equal to the insert location, as flushing to disk happens after insertion into WAL buffers
- The function is accessible via SQL as a system function
- Located in `src/backend/access/transam/xlogfuncs.c:315-336`