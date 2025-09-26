# pg_stat_get_backend_wait_event_type

## Location
[src/backend/utils/adt/pgstatfuncs.c:766-786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L766-L786)

## Overview
Returns the wait event type name for a specific backend process, providing information about what kind of resource or operation the backend is currently waiting for.

## Definition
```c
Datum pg_stat_get_backend_wait_event_type(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the wait event type string for a backend process identified by its process number. It accesses the backend's current wait event information through the process control structure and translates the wait event info into a human-readable wait event type name. The function handles various error conditions such as unavailable backend information and insufficient privileges, returning appropriate error messages in these cases.

## Parameters / Member Variables
- `procNumber` (int32): The process number identifying the target backend process

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_beentry_by_proc_number](pgstat_get_beentry_by_proc_number.md)
  - HAS_PGSTAT_PERMISSIONS
  - [BackendPidGetProc](../B/BackendPidGetProc.md)
  - [pgstat_get_wait_event_type](pgstat_get_wait_event_type.md)
  - [cstring_to_text](../c/cstring_to_text.md)
  - PG_RETURN_TEXT_P
- Data types used:
  - [PgBackendStatus](../P/PgBackendStatus.md)
  - [PGPROC](../P/PGPROC.md)

## Notes and Other Information
- Returns NULL if no wait event type is available
- Returns "<backend information not available>" if the backend entry cannot be found
- Returns "<insufficient privilege>" if the user lacks permission to view the backend's information
- This function is typically used by system monitoring tools and the pg_stat_activity view to display wait event information