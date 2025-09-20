# pg_last_wal_replay_lsn

## Location
[src/backend/access/transam/xlogfuncs.c:356-372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogfuncs.c#L356-L372)

## Overview
Returns the last WAL replay location, indicating the position up to which WAL records have been replayed during recovery.

## Definition
```c
Datum pg_last_wal_replay_lsn(PG_FUNCTION_ARGS)
```

## Detailed Description
This function reports the last WAL (Write-Ahead Log) replay location in the same format as functions like `pg_backup_start`. This function is particularly useful for determining how much of WAL is visible to read-only connections during recovery operations.

The replay location indicates the progress of WAL record replay during recovery processes, whether from archive recovery, streaming replication, or other recovery scenarios. This information is crucial for understanding the consistency point that read-only queries can see during recovery. If no WAL has been replayed (recptr is 0), the function returns NULL.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [GetXLogReplayRecPtr](../G/GetXLogReplayRecPtr.md)() - Retrieves the current WAL replay record pointer (with NULL parameter)
  - `PG_RETURN_LSN` - Macro to return LSN value to PostgreSQL function caller
- Called from (representative examples):
  - No direct callers found in the codebase (SQL-callable function)

## Notes and Other Information
- This function is useful for determining how much of WAL is visible to read-only connections during recovery
- Returns NULL if no WAL has been replayed (when recptr is 0)
- Does not perform recovery status checks, as it is specifically designed to work during recovery scenarios
- Essential for monitoring recovery progress in standby servers and hot standby scenarios
- The function is accessible via SQL as a system function
- Located in `src/backend/access/transam/xlogfuncs.c:356-372`
- The returned LSN represents the last successfully replayed WAL record, defining the consistency point for read-only queries