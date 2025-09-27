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

## Simplified Source

```c
// Simplified version of pq_setkeepalivescount
int pq_setkeepalivescount(int count, Port *port) {
    // Skip if not a TCP connection
    if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
        return STATUS_OK;

#ifdef TCP_KEEPCNT
    // Skip if value hasn't changed
    if (count == port->keepalives_count)
        return STATUS_OK;

    // Get default value if not already known
    if (port->default_keepalives_count <= 0) {
        if (pq_getkeepalivescount(port) < 0) {
            // Handle unknown defaults
            return (count == 0) ? STATUS_OK : STATUS_ERROR;
        }
    }

    // Use default value if count is 0
    if (count == 0)
        count = port->default_keepalives_count;

    // Apply the new keep-alive count setting
    if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPCNT,
                   (char *) &count, sizeof(count)) < 0) {
        ereport(LOG, (errmsg("setsockopt(TCP_KEEPCNT) failed: %m")));
        return STATUS_ERROR;
    }

    // Update port state
    port->keepalives_count = count;
#else
    // TCP_KEEPCNT not supported on this platform
    if (count != 0) {
        ereport(LOG, (errmsg("setsockopt(TCP_KEEPCNT) not supported")));
        return STATUS_ERROR;
    }
#endif

    return STATUS_OK;
}
```

Key simplifications made:
- Consolidated error handling logic for clarity
- Added descriptive comments for each major logic block
- Simplified conditional expressions
- Focused on the main execution path
- Abstracted low-level socket operation details
- Maintained all essential functionality and error cases