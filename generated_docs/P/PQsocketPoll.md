# PQsocketPoll

## Location
src/interfaces/libpq/fe-misc.c: 1117 - 1210

## Overview
PQsocketPoll is a cross-platform socket polling function that checks a file descriptor for read and/or write data availability, with configurable timeout support.

## Definition


## Detailed Description
This function provides a unified interface for socket polling across different platforms, internally using either poll(2) or select(2) depending on system availability (controlled by HAVE_POLL). It allows checking a socket for readability, writability, or both, with precise timeout control specified in microseconds since Unix epoch. The function handles timeout computation and converts between different time representations as needed for the underlying system calls.

## Parameters / Member Variables
- : The socket file descriptor to monitor
- : Flag indicating whether to check for read availability (non-zero enables)
- : Flag indicating whether to check for write availability (non-zero enables)  
- : Timeout as microseconds since Unix epoch (-1 for infinite, 0 for immediate)

## Dependencies
- Functions called/Symbols referenced:
  - [PQgetCurrentTimeUSec](PQgetCurrentTimeUSec.md)
  - poll (when HAVE_POLL is defined)
  - select (when HAVE_POLL is not defined)
  - pg_usec_time_t (time type)
- Called from (representative examples):
  - [wait_until_connected](../w/wait_until_connected.md) (src/bin/psql/command.c:3890)
  - [pqSocketCheck](../p/pqSocketCheck.md) (src/interfaces/libpq/fe-misc.c:1090)

## Notes and Other Information
- Returns >0 if the specified condition is met, 0 on timeout, -1 on error/interrupt
- If neither forRead nor forWrite are set, immediately returns 0 (timeout condition)
- Uses poll(2) when available for better performance, falls back to select(2) otherwise
- Handles timeout conversion from microsecond precision to milliseconds (poll) or seconds/microseconds (select)
- Part of libpq's internal socket management infrastructure