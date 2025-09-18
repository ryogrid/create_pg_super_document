# wait_on_socket_set

## Location
[src/bin/pgbench/pgbench.c:7940-7956](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L7940-L7956)

## Overview
Waits for input activity on a set of sockets using the select() system call, with optional timeout support.

## Definition
static int wait_on_socket_set(socket_set *sa, int64 usecs)

## Detailed Description
This function uses the select() system call to monitor a set of socket file descriptors for read activity. It supports both blocking and non-blocking operation based on the timeout parameter. When a positive timeout is specified, it converts the microsecond value to a timeval structure for use with select(). If no timeout is specified (usecs <= 0), it performs a blocking select() call that waits indefinitely until at least one socket has input available. The function returns the number of ready file descriptors or -1 on error, following standard select() semantics.

## Parameters / Member Variables
- `sa`: Pointer to the socket_set containing the file descriptors to monitor and the maximum fd value
- `usecs`: Timeout in microseconds; if > 0, enables timeout; if <= 0, waits indefinitely

## Dependencies
- Functions called/Symbols referenced:
  - select (system call)
  - timeval (system structure)
- Called from (representative examples):
  - [threadRun](../t/threadRun.md) (multiple call sites for socket monitoring in pgbench)

## Notes and Other Information
- Returns the same values as select(): number of ready descriptors, 0 for timeout, or -1 for error
- Timeout conversion handles microseconds by splitting into tv_sec and tv_usec components
- Only monitors for read readiness (input activity) - write and exception sets are NULL
- Part of pgbench's asynchronous I/O handling for managing multiple concurrent database connections
- Uses sa->maxfd + 1 as the nfds parameter to select(), following POSIX requirements