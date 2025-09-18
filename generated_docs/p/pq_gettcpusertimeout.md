# pq_gettcpusertimeout

## Location
[src/backend/libpq/pqcomm.c:1873-1902](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1873-L1902)

## Overview
Retrieves the TCP user timeout setting for a PostgreSQL connection port, which specifies the total time for unacknowledged data to remain outstanding before the connection is forcefully closed.

## Definition
```c
int pq_gettcpusertimeout(Port *port)
```

## Detailed Description
This function returns the TCP user timeout (TCP_USER_TIMEOUT) for the specified port. The TCP user timeout specifies the maximum amount of time that transmitted data may remain unacknowledged before the TCP connection is forcefully closed. Unlike keep-alive settings that only work on idle connections, user timeout applies to active connections with outstanding data. The function first checks if a custom value has been set for the port, and if not, retrieves and caches the system default value using getsockopt(). For Unix domain sockets or when TCP_USER_TIMEOUT is not supported, it returns 0.

The function implements a caching mechanism similar to keep-alive functions to avoid repeated system calls by storing the default system value in port->default_tcp_user_timeout after the first retrieval.

## Parameters / Member Variables
- `port`: Pointer to the Port structure representing the client connection. If NULL or represents a Unix domain socket, the function returns 0.

## Dependencies
- Functions called/Symbols referenced:
  - getsockopt (system call to retrieve socket options)
  - ereport (PostgreSQL error reporting function)
  - [errmsg](../e/errmsg.md) (PostgreSQL error message formatting)
- Called from (representative examples):
  - [pq_settcpusertimeout](pq_settcpusertimeout.md)
  - [show_tcp_user_timeout](../s/show_tcp_user_timeout.md)

## Notes and Other Information
- The function is only functional when TCP_USER_TIMEOUT is defined at compile time
- Returns 0 for Unix domain sockets since TCP user timeout doesn't apply
- Uses a caching mechanism to store the default system value to avoid repeated getsockopt calls
- If getsockopt fails, it logs an error and sets the default to -1 to indicate unknown status
- TCP user timeout is measured in milliseconds and provides more aggressive connection failure detection than keep-alive alone
- The user timeout setting works independently of keep-alive settings and can detect connection failures even during active data transmission