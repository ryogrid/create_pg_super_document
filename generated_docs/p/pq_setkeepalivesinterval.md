# pq_setkeepalivesinterval

## Location
[src/backend/libpq/pqcomm.c:1749-1797](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1749-L1797)

## Overview
Sets the TCP keepalive interval value (time between keepalive probes) for a given port connection using platform-specific implementations.

## Definition

```c
int
pq_setkeepalivesinterval(int interval, Port *port)
```
## Detailed Description
This function configures the TCP keepalive interval for a port connection. The interval determines the time between successive keepalive probes after the first probe is sent. On Unix/Linux systems, it uses setsockopt() with TCP_KEEPINTVL. On Windows, it delegates to pq_setkeepaliveswin32() with the current idle setting. The function handles value validation, retrieves system defaults when needed, and gracefully handles platforms that don't support keepalive interval configuration. A value of 0 uses the system default.

## Parameters / Member Variables
- `interval`: Interval value in seconds between keepalive probes (0 = use system default)
- `port`: Pointer to Port structure containing socket and keepalive configuration

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getkeepalivesinterval](pq_getkeepalivesinterval.md) (to retrieve defaults)
  - [pq_setkeepaliveswin32](pq_setkeepaliveswin32.md) (Windows implementation)
  - setsockopt (system socket API, Unix/Linux only)
  - ereport (PostgreSQL logging)
  - STATUS_OK (PostgreSQL status constant)
  - STATUS_ERROR (PostgreSQL status constant)
  - TCP_KEEPINTVL (TCP keepalive interval socket option constant)
  - IPPROTO_TCP (protocol constant)
- Called from (representative examples):
  - [pq_init](pq_init.md)
  - [assign_tcp_keepalives_interval](../a/assign_tcp_keepalives_interval.md)

## Notes and Other Information
- Platform-dependent implementation with conditional compilation
- Returns STATUS_OK immediately for Unix domain sockets (no-op)
- On Unix/Linux: uses setsockopt() with TCP_KEEPINTVL
- On Windows: delegates to pq_setkeepaliveswin32() with current idle setting
- Updates port->keepalives_interval on successful configuration
- Returns STATUS_ERROR on unsupported platforms when interval != 0
- Handles default value retrieval and validation logic
- Short-circuits if requested value matches current setting
- Error message differs slightly from idle version for better diagnostics

## Simplified Source

```c
// Simplified version of pq_setkeepalivesinterval
int pq_setkeepalivesinterval(int interval, Port *port) {
    // Skip for Unix domain sockets - no TCP keepalive needed
    if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
        return STATUS_OK;

#if defined(TCP_KEEPINTVL) || defined(SIO_KEEPALIVE_VALS)
    // Skip if already set to requested value
    if (interval == port->keepalives_interval)
        return STATUS_OK;

#ifndef WIN32
    // Unix/Linux implementation
    // Get system default if needed
    if (port->default_keepalives_interval <= 0) {
        if (pq_getkeepalivesinterval(port) < 0) {
            return (interval == 0) ? STATUS_OK : STATUS_ERROR;
        }
    }

    // Use system default if interval is 0
    if (interval == 0)
        interval = port->default_keepalives_interval;

    // Set the TCP keepalive interval
    if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPINTVL,
                   (char *) &interval, sizeof(interval)) < 0) {
        ereport(LOG, (errmsg("setsockopt(TCP_KEEPINTVL) failed: %m")));
        return STATUS_ERROR;
    }

    // Update cached value
    port->keepalives_interval = interval;
#else
    // Windows implementation - delegate to Windows-specific function
    return pq_setkeepaliveswin32(port, port->keepalives_idle, interval);
#endif

#else
    // Platform doesn't support TCP keepalive intervals
    if (interval != 0) {
        ereport(LOG, (errmsg("setsockopt(TCP_KEEPINTVL) not supported")));
        return STATUS_ERROR;
    }
#endif

    return STATUS_OK;
}
```

Key simplifications made:
- Removed detailed conditional compilation complexity for clarity
- Consolidated error handling paths
- Added descriptive comments for each major logic section
- Simplified the default value retrieval logic
- Focused on the main execution paths for Unix/Linux and Windows
- Preserved the essential TCP keepalive interval configuration logic