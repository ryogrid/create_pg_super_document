# pq_setkeepalivescount

## Location
[src/backend/libpq/pqcomm.c:1828-1872](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1828-L1872)

## Overview
Sets the TCP keep-alive probe count for a PostgreSQL connection port, configuring how many unanswered keep-alive probes are sent before considering the connection dead.

## Definition
```c
int pq_setkeepalivescount(int count, Port *port)
```

## Detailed Description
This function configures the TCP keep-alive count (TCP_KEEPCNT) for the specified port. The keep-alive count determines how many consecutive failed keep-alive probes are sent before the TCP connection is considered dead and closed. If count is 0, the function uses the system default value. The function validates the current setting before making changes and handles various edge cases including unsupported platforms and unknown default values.

The function performs several validation checks: it verifies the port is valid and not a Unix domain socket, ensures the new value differs from the current setting, and retrieves default values if needed before applying the change via setsockopt().

## Parameters / Member Variables
- `count`: The desired keep-alive probe count. If 0, the system default value will be used.
- `port`: Pointer to the Port structure representing the client connection. If NULL or represents a Unix domain socket, the function returns STATUS_OK without making changes.

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getkeepalivescount](pq_getkeepalivescount.md) (to retrieve current/default values)
  - setsockopt (system call to set socket options)
  - ereport (PostgreSQL error reporting function)
  - [errmsg](../e/errmsg.md) (PostgreSQL error message formatting)
  - STATUS_OK, STATUS_ERROR (return value constants)
- Called from (representative examples):
  - [pq_init](pq_init.md) (during connection initialization)
  - [assign_tcp_keepalives_count](../a/assign_tcp_keepalives_count.md) (GUC assignment hook)

## Notes and Other Information
- Returns STATUS_OK on success, STATUS_ERROR on failure
- Only functional when TCP_KEEPCNT is defined at compile time
- Returns STATUS_OK immediately for Unix domain sockets since TCP keep-alive doesn't apply
- Optimizes by avoiding redundant setsockopt calls when the value hasn't changed
- Uses a special case where count=0 means "use system default"
- If the system default is unknown (negative value), setting to 0 succeeds but non-zero values fail
- Logs errors when setsockopt fails or when TCP_KEEPCNT is not supported on the platform