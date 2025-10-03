# PQgetCurrentTimeUSec

## Location
[src/interfaces/libpq/fe-misc.c:1211-1230](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L1211-L1230)

## Overview
PQgetCurrentTimeUSec provides a platform-independent way to get the current time with microsecond precision, primarily used for timeout calculations in socket operations.

## Definition
```c
pg_usec_time_t PQgetCurrentTimeUSec(void)
```

## Detailed Description
This function wraps the system's gettimeofday() function to provide a consistent interface for obtaining high-precision timestamps across different platforms. It returns the current time as microseconds since the Unix epoch, which is the standard format used throughout libpq for timeout specifications. The function converts the timeval structure returned by gettimeofday() into a single 64-bit integer representing total microseconds.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [gettimeofday](../g/gettimeofday.md) (system call)
  - pg_usec_time_t (time type)
- Called from (representative examples):
  - [wait_until_connected](../w/wait_until_connected.md) (src/bin/psql/command.c:3889)
  - [pqConnectDBComplete](../p/pqConnectDBComplete.md) (src/interfaces/libpq/fe-connect.c:2509)
  - [PQsocketPoll](PQsocketPoll.md) (src/interfaces/libpq/fe-misc.c:1143, 1184)

## Notes and Other Information
- Returns time as pg_usec_time_t (microseconds since Unix epoch)
- Specifically designed as a reference value for PQsocketPoll's timeout parameter
- Provides platform independence by abstracting the underlying time retrieval mechanism
- Used internally by libpq for connection timeouts and socket polling operations

## Simplified Source

```c
pg_usec_time_t PQgetCurrentTimeUSec(void)
{
    struct timeval tval;

    // Get current time with microsecond precision
    gettimeofday(&tval, NULL);

    // Convert to microseconds since Unix epoch
    return (pg_usec_time_t) tval.tv_sec * 1000000 + tval.tv_usec;
}
```