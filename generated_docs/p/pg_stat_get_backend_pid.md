# pg_stat_get_backend_pid

## Location
src/backend/utils/adt/pgstatfuncs.c: 668 - 680

## Overview
Returns the process ID (PID) of a specific PostgreSQL backend process identified by its process number.

## Definition
```c
Datum pg_stat_get_backend_pid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the process ID of a PostgreSQL backend process by its process number (procNumber). Unlike pg_backend_pid() which returns the PID of the current backend, this function can retrieve the PID of any active backend process. It accesses the shared memory backend status array to find the backend entry corresponding to the given process number and returns its st_procpid field. If no backend exists with the specified process number, the function returns NULL.

## Parameters / Member Variables
- `procNumber` (int32): The process number identifying which backend's PID to retrieve. This is an index into the backend status array.

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_get_beentry_by_proc_number: Retrieves backend status entry by process number
  - PgBackendStatus: Structure containing backend status information
  - PG_GETARG_INT32: Macro to extract int32 argument from function call
  - PG_RETURN_INT32: Macro to return int32 value from PostgreSQL function
  - PG_RETURN_NULL: Macro to return NULL from PostgreSQL function

## Notes and Other Information
- Returns NULL if the specified process number does not correspond to an active backend
- The process number is not the same as the operating system PID - it's an internal PostgreSQL identifier
- This function is used internally by system views like pg_stat_activity to display backend process information
- The function accesses shared memory data structures that track all active backend processes
- Located in src/backend/utils/adt/pgstatfuncs.c:668-680