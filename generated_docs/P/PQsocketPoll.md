# PQsocketPoll

## Location
[src/interfaces/libpq/fe-misc.c:1117-1210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L1117-L1210)

## Overview
PQsocketPoll is a cross-platform socket polling function that checks a file descriptor for read and/or write data availability, with configurable timeout support.

## Definition

```c
struct pollfd input_fd;
```
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

## Simplified Source
```c
int PQsocketPoll(int sock, int forRead, int forWrite, pg_usec_time_t end_time) {
    // Return immediately if neither read nor write requested
    if (!forRead && !forWrite)
        return 0;

#ifdef HAVE_POLL
    // Use poll() when available
    struct pollfd input_fd;
    input_fd.fd = sock;
    input_fd.events = POLLERR;

    if (forRead) input_fd.events |= POLLIN;
    if (forWrite) input_fd.events |= POLLOUT;

    // Convert timeout from microseconds to milliseconds
    int timeout_ms;
    if (end_time == -1)
        timeout_ms = -1; // Infinite
    else if (end_time == 0)
        timeout_ms = 0;  // Immediate
    else {
        pg_usec_time_t now = PQgetCurrentTimeUSec();
        timeout_ms = (end_time > now) ? (end_time - now) / 1000 : 0;
    }

    return poll(&input_fd, 1, timeout_ms);

#else
    // Fallback to select() when poll() not available
    fd_set input_mask, output_mask, except_mask;
    FD_ZERO(&input_mask);
    FD_ZERO(&output_mask);
    FD_ZERO(&except_mask);

    if (forRead) FD_SET(sock, &input_mask);
    if (forWrite) FD_SET(sock, &output_mask);
    FD_SET(sock, &except_mask);

    // Convert timeout to timeval structure
    struct timeval timeout, *ptr_timeout;
    if (end_time == -1) {
        ptr_timeout = NULL; // Infinite
    } else {
        pg_usec_time_t now = PQgetCurrentTimeUSec();
        if (end_time > now) {
            timeout.tv_sec = (end_time - now) / 1000000;
            timeout.tv_usec = (end_time - now) % 1000000;
        } else {
            timeout.tv_sec = 0;
            timeout.tv_usec = 0;
        }
        ptr_timeout = &timeout;
    }

    return select(sock + 1, &input_mask, &output_mask, &except_mask, ptr_timeout);
#endif
}
```