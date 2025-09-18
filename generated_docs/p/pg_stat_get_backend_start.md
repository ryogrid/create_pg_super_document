# pg_stat_get_backend_start

## Location
src/backend/utils/adt/pgstatfuncs.c: 857 - 878

## Overview
Returns the timestamp when a specific backend process was started.

## Definition
```c
Datum pg_stat_get_backend_start(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the start timestamp of a backend process identified by its process number. It accesses the backend status entry and returns the st_proc_start_timestamp field, which records when the backend process was initially started. Unlike activity or transaction start times, this timestamp represents the lifetime start of the backend process itself. The function returns NULL when backend information is unavailable or when the user lacks sufficient privileges.

## Parameters / Member Variables
- `procNumber` (int32): The process number identifying the target backend process

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_beentry_by_proc_number](pgstat_get_beentry_by_proc_number.md)
  - HAS_PGSTAT_PERMISSIONS  
  - PG_RETURN_TIMESTAMPTZ
- Data types used:
  - [PgBackendStatus](../P/PgBackendStatus.md)
  - TimestampTz

## Notes and Other Information
- Returns NULL if backend information is not available
- Returns NULL if the user has insufficient privileges to view the backend information
- Returns NULL if st_proc_start_timestamp is 0, though the code comment suggests this "probably can't happen"
- This timestamp represents when the backend process itself started, which is the earliest of all the start timestamps
- Used by pg_stat_activity view to show backend process start times
- Helps identify long-running backend processes and analyze process lifecycle
- This is different from activity_start (current query) and xact_start (current transaction) timestamps
- Useful for monitoring backend process uptime and identifying processes that may need attention