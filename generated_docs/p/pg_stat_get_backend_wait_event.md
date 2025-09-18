# pg_stat_get_backend_wait_event

## Location
src/backend/utils/adt/pgstatfuncs.c: 787 - 808

## Overview
Returns the specific wait event name for a backend process, providing detailed information about the exact resource or operation the backend is currently waiting for.

## Definition
```c
Datum pg_stat_get_backend_wait_event(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the specific wait event name for a backend process identified by its process number. While pg_stat_get_backend_wait_event_type returns the category of wait event, this function returns the specific wait event within that category. It accesses the backend's current wait event information through the process control structure and translates it into a human-readable wait event name. The function handles error conditions such as unavailable backend information and insufficient privileges.

## Parameters / Member Variables
- `procNumber` (int32): The process number identifying the target backend process

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_get_beentry_by_proc_number
  - HAS_PGSTAT_PERMISSIONS
  - BackendPidGetProc
  - pgstat_get_wait_event
  - cstring_to_text
  - PG_RETURN_TEXT_P
- Data types used:
  - PgBackendStatus
  - PGPROC

## Notes and Other Information
- Returns NULL if no specific wait event is available
- Returns "<backend information not available>" if the backend entry cannot be found
- Returns "<insufficient privilege>" if the user lacks permission to view the backend's information
- This function provides more granular information than pg_stat_get_backend_wait_event_type
- Used by system monitoring tools and the pg_stat_activity view to display detailed wait event information