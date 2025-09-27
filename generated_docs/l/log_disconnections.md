# log_disconnections

## Location
[src/backend/tcop/postgres.c:5196-5231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L5196-L5231)

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
  - [Port](../P/Port.md) (structure containing client connection information)
  - MyProcPort (global variable pointing to current connection's Port structure)
  - MyStartTimestamp (global variable storing session start time)
  - [TimestampDifference](../T/TimestampDifference.md) (function to calculate time difference)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) (function to get current timestamp)
  - ereport (PostgreSQL logging function)
  - [errmsg](../e/errmsg.md) (error message formatting function)
  - SECS_PER_HOUR (constant for seconds per hour)
  - SECS_PER_MINUTE (constant for seconds per minute)

- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (registered as exit handler)

## Notes and Other Information
- Registered as a process exit handler in `PostgresMain`
- Automatically invoked when backend process terminates for any reason
- Provides session duration in human-readable format (HH:MM:SS.mmm)
- Logs at LOG level, making it visible in server logs
- Essential for connection auditing and session monitoring
- Includes detailed client connection metadata for security and debugging
- Works for both normal and abnormal process termination
- Part of PostgreSQL's comprehensive logging and monitoring infrastructure

## Simplified Source

```c
// Simplified version of log_disconnections
static void log_disconnections(int code, Datum arg) {
    Port *port = MyProcPort;
    long secs;
    int usecs;

    // Calculate session duration from start to now
    TimestampDifference(MyStartTimestamp, GetCurrentTimestamp(), &secs, &usecs);

    // Convert to human-readable time format (hours:minutes:seconds.milliseconds)
    int hours = secs / SECS_PER_HOUR;
    int minutes = (secs % SECS_PER_HOUR) / SECS_PER_MINUTE;
    int seconds = secs % SECS_PER_MINUTE;
    int msecs = usecs / 1000;

    // Log disconnection with session time and connection details
    ereport(LOG,
        (errmsg("disconnection: session time: %d:%02d:%02d.%03d "
                "user=%s database=%s host=%s%s%s",
                hours, minutes, seconds, msecs,
                port->user_name, port->database_name, port->remote_host,
                port->remote_port[0] ? " port=" : "", port->remote_port)));
}
```

Key simplifications made:
- Consolidated time calculation variables into logical groups
- Combined modulo operations for cleaner time conversion
- Added explanatory comments for each major step
- Preserved the exact logging format and all essential functionality
- Maintained the same algorithm flow with clearer variable organization