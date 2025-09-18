# pg_stat_get_backend_activity_start

## Location
src/backend/utils/adt/pgstatfuncs.c: 809 - 834

## Overview
Returns the timestamp when the current activity (query) started executing for a specific backend process.

## Definition
```c
Datum pg_stat_get_backend_activity_start(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the start timestamp of the currently executing activity (query) for a backend process identified by its process number. It accesses the backend status entry and returns the st_activity_start_timestamp field, which records when the current query began execution. The function returns NULL in several cases: when backend information is unavailable, when the user lacks sufficient privileges, or when query-level statistics collection is not enabled.

## Parameters / Member Variables
- `procNumber` (int32): The process number identifying the target backend process

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_get_beentry_by_proc_number
  - HAS_PGSTAT_PERMISSIONS
  - PG_RETURN_TIMESTAMPTZ
- Data types used:
  - PgBackendStatus
  - TimestampTz

## Notes and Other Information
- Returns NULL if backend information is not available
- Returns NULL if the user has insufficient privileges to view the backend information
- Returns NULL if query-level statistics collection is disabled (st_activity_start_timestamp == 0)
- The timestamp represents when the current query/activity started, not when the backend process started
- Used by pg_stat_activity view to show query start times
- Requires appropriate statistics collection settings to be enabled for meaningful results