# pgstat_get_beentry_by_proc_number

## Location
[src/backend/utils/activity/backend_status.c:1072-1096](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_status.c#L1072-L1096)

## Overview
Retrieves the current activity entry for a specific backend process identified by its process number, providing access to the backend's status information for SQL-callable statistics functions.

## Definition


## Detailed Description
This function serves as a support function for PostgreSQL's SQL-callable pgstat* functions (like pg_stat_get_backend_*). It returns the local copy of the current-activity entry for one backend session, or NULL if the given process number doesn't identify any known session. The function acts as a wrapper around pgstat_get_local_beentry_by_proc_number(), extracting the PgBackendStatus structure from the LocalPgBackendStatus wrapper.

## Parameters / Member Variables
- `procNumber`: The ProcNumber of the desired backend session (process number identifier)

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_local_beentry_by_proc_number](pgstat_get_local_beentry_by_proc_number.md)
  - ProcNumber (type)
  - [LocalPgBackendStatus](../L/LocalPgBackendStatus.md) (struct type)
- Called from (representative examples):
  - [pg_stat_get_backend_pid](pg_stat_get_backend_pid.md)
  - [pg_stat_get_backend_dbid](pg_stat_get_backend_dbid.md)
  - [pg_stat_get_backend_userid](pg_stat_get_backend_userid.md)
  - [pg_stat_get_backend_activity](pg_stat_get_backend_activity.md)
  - [pg_stat_get_backend_wait_event_type](pg_stat_get_backend_wait_event_type.md)
  - [pg_stat_get_backend_wait_event](pg_stat_get_backend_wait_event.md)
  - [pg_stat_get_backend_activity_start](pg_stat_get_backend_activity_start.md)
  - [pg_stat_get_backend_xact_start](pg_stat_get_backend_xact_start.md)
  - [pg_stat_get_backend_start](pg_stat_get_backend_start.md)
  - [pg_stat_get_backend_client_addr](pg_stat_get_backend_client_addr.md)
  - [pg_stat_get_backend_client_port](pg_stat_get_backend_client_port.md)

## Notes and Other Information
- Unlike pgstat_get_local_beentry_by_index(), this function takes a ProcNumber rather than an array index
- The caller is responsible for checking if the user has permission to view the returned information, especially query strings
- Returns NULL if the specified process number is not found or doesn't correspond to an active session
- Part of PostgreSQL's statistics collection system, enabling monitoring of backend processes through SQL functions
- The returned PgBackendStatus pointer provides access to detailed information about the backend's current state and activity