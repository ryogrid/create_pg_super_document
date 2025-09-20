# pg_last_wal_receive_lsn

## Location
[src/backend/access/transam/xlogfuncs.c:337-355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogfuncs.c#L337-L355)

## Overview
Returns the last WAL receive location, indicating the position up to which WAL data has been received and synced to disk by the walreceiver process.

## Definition
```c
Datum pg_last_wal_receive_lsn(PG_FUNCTION_ARGS)
```

## Detailed Description
This function reports the last WAL (Write-Ahead Log) receive location in the same format as functions like `pg_backup_start`. This function is particularly useful for determining how much WAL data is guaranteed to be received and synchronized to disk by the walreceiver process in streaming replication scenarios.

Unlike the previous WAL control functions, this function does not check for recovery status, as it is specifically designed to work in replication contexts where WAL is being received from a primary server. If no WAL has been received (recptr is 0), the function returns NULL.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [GetWalRcvFlushRecPtr](../G/GetWalRcvFlushRecPtr.md)() - Retrieves the WAL receiver flush record pointer (with NULL, NULL parameters)
  - `PG_RETURN_LSN` - Macro to return LSN value to PostgreSQL function caller
- Called from (representative examples):
  - No direct callers found in the codebase (SQL-callable function)

## Notes and Other Information
- This function is useful for determining how much of WAL is guaranteed to be received and synced to disk by walreceiver
- Returns NULL if no WAL has been received (when recptr is 0)
- Does not perform recovery status checks, unlike other WAL control functions
- Primarily used in streaming replication monitoring and debugging
- The function is accessible via SQL as a system function
- Located in `src/backend/access/transam/xlogfuncs.c:337-355`
- The returned LSN represents data that has been both received from the primary and flushed to disk