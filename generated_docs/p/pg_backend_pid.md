# pg_backend_pid

## Location
[src/backend/utils/adt/pgstatfuncs.c:661-667](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L661-L667)

## Overview
Returns the process ID (PID) of the current PostgreSQL backend process.

## Definition
```c
Datum pg_backend_pid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a simple PostgreSQL system function that returns the process ID of the current backend process. It serves as a SQL-callable wrapper around the global variable MyProcPid, which contains the operating system process ID of the current PostgreSQL backend. This function is useful for monitoring, debugging, and administrative purposes where users need to identify which backend process is executing their query.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - MyProcPid (global variable containing current process PID)
  - PG_RETURN_INT32 (macro for returning integer values from PostgreSQL functions)

## Notes and Other Information
- This function always returns the PID of the current backend process that is executing the function call
- The returned value is of type int32, representing the operating system process ID
- Commonly used in monitoring queries and administrative scripts to track backend processes
- The function is defined in src/backend/utils/adt/pgstatfuncs.c:661-664

## Simplified Source

```c
Datum pg_backend_pid(PG_FUNCTION_ARGS)
{
    // Return the current backend process ID
    PG_RETURN_INT32(MyProcPid);
}
```