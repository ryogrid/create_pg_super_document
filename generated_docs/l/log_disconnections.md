# log_disconnections

## Location
src/backend/tcop/postgres.c: 5196 - 5231

## Overview
This function serves as an `on_proc_exit` handler that logs session disconnection information, including session duration and client connection details when a PostgreSQL backend process terminates.

## Definition
```c
static void log_disconnections(int code, Datum arg)
```

## Detailed Description
`log_disconnections` is a callback function registered with the process exit handler system to automatically log disconnection events. When a backend process terminates (normally or abnormally), this function calculates the total session duration by comparing the current timestamp with `MyStartTimestamp`. It formats the duration in hours:minutes:seconds.milliseconds format and logs comprehensive connection information including username, database name, remote host, and port. This provides valuable audit trail information for connection tracking and debugging purposes.

## Parameters / Member Variables
- `code`: Exit code of the terminating process (standard on_proc_exit parameter)
- `arg`: Additional argument data (standard on_proc_exit parameter, unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - Port (structure containing client connection information)
  - MyProcPort (global variable pointing to current connection's Port structure)
  - MyStartTimestamp (global variable storing session start time)
  - TimestampDifference (function to calculate time difference)
  - GetCurrentTimestamp (function to get current timestamp)
  - ereport (PostgreSQL logging function)
  - errmsg (error message formatting function)
  - SECS_PER_HOUR (constant for seconds per hour)
  - SECS_PER_MINUTE (constant for seconds per minute)

- Called from (representative examples):
  - PostgresMain (registered as exit handler)

## Notes and Other Information
- Registered as a process exit handler in `PostgresMain`
- Automatically invoked when backend process terminates for any reason
- Provides session duration in human-readable format (HH:MM:SS.mmm)
- Logs at LOG level, making it visible in server logs
- Essential for connection auditing and session monitoring
- Includes detailed client connection metadata for security and debugging
- Works for both normal and abnormal process termination
- Part of PostgreSQL's comprehensive logging and monitoring infrastructure