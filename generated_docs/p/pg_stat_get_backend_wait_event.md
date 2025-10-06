# pg_stat_get_backend_wait_event

## Location
[src/backend/utils/adt/pgstatfuncs.c:787-808](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L787-L808)

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
  - [pgstat_get_beentry_by_proc_number](pgstat_get_beentry_by_proc_number.md)
  - HAS_PGSTAT_PERMISSIONS
  - [BackendPidGetProc](../B/BackendPidGetProc.md)
  - [pgstat_get_wait_event](pgstat_get_wait_event.md)
  - [cstring_to_text](../c/cstring_to_text.md)
  - PG_RETURN_TEXT_P
- Data types used:
  - [PgBackendStatus](../P/PgBackendStatus.md)
  - [PGPROC](../P/PGPROC.md)

## Notes and Other Information
- Returns NULL if no specific wait event is available
- Returns "<backend information not available>" if the backend entry cannot be found
- Returns "<insufficient privilege>" if the user lacks permission to view the backend's information
- This function provides more granular information than pg_stat_get_backend_wait_event_type
- Used by system monitoring tools and the pg_stat_activity view to display detailed wait event information

## Simplified Source

```c
Datum
pg_stat_get_backend_wait_event(PG_FUNCTION_ARGS)
{
    int32 proc_number = PG_GETARG_INT32(0);
    PgBackendStatus *backend_entry;
    PGPROC *proc;
    const char *wait_event = NULL;

    // Get backend status entry for the specified process
    backend_entry = pgstat_get_beentry_by_proc_number(proc_number);

    // Determine specific wait event based on backend status and permissions
    if (backend_entry == NULL)
        wait_event = "<backend information not available>";
    else if (!HAS_PGSTAT_PERMISSIONS(backend_entry->st_userid))
        wait_event = "<insufficient privilege>";
    else if ((proc = BackendPidGetProc(backend_entry->st_procpid)) != NULL)
        wait_event = pgstat_get_wait_event(proc->wait_event_info);

    // Return NULL if no wait event available, otherwise return as text
    if (!wait_event)
        PG_RETURN_NULL();

    PG_RETURN_TEXT_P(cstring_to_text(wait_event));
}
```