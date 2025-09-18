# pg_postmaster_start_time

## Location
[src/backend/utils/adt/timestamp.c:1636-1641](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1636-L1641)

## Overview
Returns the timestamp when the PostgreSQL postmaster (main server process) was started, providing a fixed reference point for server uptime calculations.

## Definition
```c
Datum pg_postmaster_start_time(PG_FUNCTION_ARGS)
```

## Detailed Description
The `pg_postmaster_start_time` function is a PostgreSQL built-in function that returns the timestamp with time zone indicating when the PostgreSQL server (postmaster process) was started. This timestamp is captured during server initialization and remains constant throughout the server's lifetime, making it useful for calculating server uptime, monitoring server restarts, and administrative purposes.

The function simply returns the value of the global variable `PgStartTime`, which is set during postmaster startup and stores the server start time. This provides a reliable and consistent way to determine when the current PostgreSQL instance began running.

## Parameters / Member Variables
This function takes no parameters (uses `PG_FUNCTION_ARGS` macro for PostgreSQL function interface).

## Dependencies
- Functions called/Symbols referenced:
  - `PgStartTime`: Global variable containing the postmaster start timestamp
  - `PG_RETURN_TIMESTAMPTZ`: PostgreSQL macro to return a timestamptz value

- Called from (representative examples):
  - SQL queries using the `pg_postmaster_start_time()` function
  - System monitoring and administrative scripts
  - Built-in function registry for SQL function dispatch

## Notes and Other Information
- The timestamp returned is with time zone (timestamptz type)
- The value is set once during postmaster startup and never changes during server runtime
- Useful for calculating server uptime by subtracting from current time
- Commonly used in monitoring systems to track server availability and restart events
- Different from connection start times or transaction start times - this is the server process start time
- The function is defined in `src/backend/utils/adt/timestamp.c` at lines 1636-1641