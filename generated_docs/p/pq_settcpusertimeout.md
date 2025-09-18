# pq_settcpusertimeout

## Location
[src/backend/libpq/pqcomm.c:1903-1950](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1903-L1950)

## Overview
Sets the TCP user timeout for a PostgreSQL connection port, configuring the maximum time that transmitted data may remain unacknowledged before the connection is forcefully closed.

## Definition
```c
int pq_settcpusertimeout(int timeout, Port *port)
```

## Detailed Description
This function configures the TCP user timeout (TCP_USER_TIMEOUT) for the specified port. The TCP user timeout specifies the maximum amount of time in milliseconds that transmitted data may remain unacknowledged before the TCP connection is forcefully closed. Unlike keep-alive settings that only work on idle connections, user timeout applies to active connections with outstanding data. If timeout is 0, the function uses the system default value. The function validates the current setting before making changes and handles various edge cases including unsupported platforms and unknown default values.

The function performs several validation checks: it verifies the port is valid and not a Unix domain socket, ensures the new value differs from the current setting, and retrieves default values if needed before applying the change via setsockopt().

## Parameters / Member Variables
- `timeout`: The desired TCP user timeout in milliseconds. If 0, the system default value will be used.
- `port`: Pointer to the Port structure representing the client connection. If NULL or represents a Unix domain socket, the function returns STATUS_OK without making changes.

## Dependencies
- Functions called/Symbols referenced:
  - [pq_gettcpusertimeout](pq_gettcpusertimeout.md) (to retrieve current/default values)
  - setsockopt (system call to set socket options)
  - ereport (PostgreSQL error reporting function)
  - [errmsg](../e/errmsg.md) (PostgreSQL error message formatting)
  - STATUS_OK, STATUS_ERROR (return value constants)
- Called from (representative examples):
  - [pq_init](pq_init.md) (during connection initialization)
  - [assign_tcp_user_timeout](../a/assign_tcp_user_timeout.md) (GUC assignment hook)

## Notes and Other Information
- Returns STATUS_OK on success, STATUS_ERROR on failure
- Only functional when TCP_USER_TIMEOUT is defined at compile time
- Returns STATUS_OK immediately for Unix domain sockets since TCP user timeout doesn't apply
- Optimizes by avoiding redundant setsockopt calls when the value hasn't changed
- Uses a special case where timeout=0 means "use system default"
- If the system default is unknown (negative value), setting to 0 succeeds but non-zero values fail
- Logs errors when setsockopt fails or when TCP_USER_TIMEOUT is not supported on the platform
- TCP user timeout provides more aggressive connection failure detection than keep-alive alone, especially for connections with active data transmission
- The timeout value is specified in milliseconds and applies to the total time for unacknowledged data