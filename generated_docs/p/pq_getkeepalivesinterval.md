# pq_getkeepalivesinterval

## Location
[src/backend/libpq/pqcomm.c:1714-1748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1714-L1748)

## Overview
Retrieves the current TCP keepalive interval value (time between keepalive probes) for a given port connection.

## Definition

```c
int
pq_getkeepalivesinterval(Port *port)
```
## Detailed Description
This function returns the TCP keepalive interval setting for a port connection. The interval determines the time between successive keepalive probes after the first probe is sent. It first checks if a custom keepalive interval has been set on the port. If not, it attempts to retrieve the system default value using getsockopt() with TCP_KEEPINTVL on non-Windows systems. On Windows platforms, the default values cannot be retrieved, so the function returns -1 to indicate "don't know". The function handles both Unix domain sockets (returns 0) and TCP sockets with appropriate platform-specific logic.

## Parameters / Member Variables
- `port`: Pointer to Port structure containing socket and keepalive configuration

## Dependencies
- Functions called/Symbols referenced:
  - getsockopt (system socket API, Unix/Linux only)
  - ereport (PostgreSQL logging)
  - TCP_KEEPINTVL (TCP keepalive interval socket option constant)
  - AF_UNIX (address family constant)
  - IPPROTO_TCP (protocol constant)
  - socklen_t (socket length type)
- Called from (representative examples):
  - [pq_setkeepalivesinterval](pq_setkeepalivesinterval.md)
  - [show_tcp_keepalives_interval](../s/show_tcp_keepalives_interval.md)

## Notes and Other Information
- Platform-dependent implementation using conditional compilation (#ifdef TCP_KEEPINTVL)
- Returns 0 for Unix domain sockets or when TCP keepalive interval is not supported
- Returns port->keepalives_interval if explicitly set
- Returns port->default_keepalives_interval or retrieves it via getsockopt() on first call
- Windows version always returns -1 for default values (cannot query defaults)
- Caches default value in port->default_keepalives_interval to avoid repeated system calls
- Uses TCP_KEEPINTVL socket option on Unix/Linux systems

## Simplified Source

```c
// Simplified version of pq_getkeepalivesinterval
int pq_getkeepalivesinterval(Port *port) {
#if defined(TCP_KEEPINTVL) || defined(SIO_KEEPALIVE_VALS)
    // Return 0 for NULL port or Unix sockets
    if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
        return 0;

    // Return cached custom value if set
    if (port->keepalives_interval != 0)
        return port->keepalives_interval;

    // Get system default if not cached
    if (port->default_keepalives_interval == 0) {
#ifndef WIN32
        socklen_t size = sizeof(port->default_keepalives_interval);

        if (getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPINTVL,
                       &port->default_keepalives_interval, &size) < 0) {
            // Log error and mark as unknown
            ereport(LOG, (errmsg("getsockopt(TCP_KEEPINTVL) failed: %m")));
            port->default_keepalives_interval = -1;
        }
#else
        // Windows cannot query defaults
        port->default_keepalives_interval = -1;
#endif
    }

    return port->default_keepalives_interval;
#else
    return 0;
#endif
}
```

Key simplifications made:
- Added explanatory comments for each logic block
- Simplified error message formatting
- Clarified Windows-specific behavior with comments
- Maintained essential platform-specific and caching logic