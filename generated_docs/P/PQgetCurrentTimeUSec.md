# PQgetCurrentTimeUSec

## Location
src/interfaces/libpq/fe-misc.c: 1211 - 1230

## Overview
PQgetCurrentTimeUSec provides a platform-independent way to get the current time with microsecond precision, primarily used for timeout calculations in socket operations.

## Definition
```c
pg_usec_time_t PQgetCurrentTimeUSec(void)
```

## Detailed Description
This function wraps the system's gettimeofday() function to provide a consistent interface for obtaining high-precision timestamps across different platforms. It returns the current time as microseconds since the Unix epoch, which is the standard format used throughout libpq for timeout specifications. The function converts the timeval structure returned by gettimeofday() into a single 64-bit integer representing total microseconds.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - gettimeofday (system call)
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