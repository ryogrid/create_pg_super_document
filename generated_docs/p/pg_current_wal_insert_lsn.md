# pg_current_wal_insert_lsn

## Location
[src/backend/access/transam/xlogfuncs.c:294-314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogfuncs.c#L294-L314)

## Overview
Returns the current WAL insert location, which represents the position where new WAL records are being inserted in the Write-Ahead Log.

## Definition

```c
Datum
pg_current_wal_insert_lsn(PG_FUNCTION_ARGS)
```
## Detailed Description
This function reports the current WAL (Write-Ahead Log) insert location in the same format as functions like . The insert location indicates where new WAL records are currently being written. This function is primarily intended for debugging purposes and provides insight into the current state of WAL record insertion.

The function ensures that it cannot be executed during recovery mode, as WAL control functions are not available when the database is in recovery state. It returns the current insert position as an LSN (Log Sequence Number) value.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  -  - Checks if database recovery is currently active
  -  - Retrieves the current WAL insert record pointer
  -  - Macro to return LSN value to PostgreSQL function caller
- Called from (representative examples):
  - No direct callers found in the codebase (SQL-callable function)

## Notes and Other Information
- This function is mostly for debugging purposes as noted in the source code comments
- The function will raise an error if called during recovery, with error code
- Returns the LSN in the standard PostgreSQL LSN format (e.g., '0/1667D48')
- The function is accessible via SQL as a system function
- Located in

## Simplified Source

```c
Datum
pg_current_wal_insert_lsn(PG_FUNCTION_ARGS)
{
    XLogRecPtr current_recptr;

    // Cannot run during recovery
    if (RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("recovery is in progress")));

    // Get current WAL insert position (for debugging)
    current_recptr = GetXLogInsertRecPtr();

    PG_RETURN_LSN(current_recptr);
}
``` 