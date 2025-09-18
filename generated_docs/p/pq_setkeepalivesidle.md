# pq_setkeepalivesidle

## Location
[src/backend/libpq/pqcomm.c:1664-1713](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1664-L1713)

## Overview
Sets the TCP keepalive idle timeout value for a given port connection using platform-specific implementations.

## Definition


## Detailed Description
This function configures the TCP keepalive idle time for a port connection. The idle time determines how long a connection must be inactive before the first keepalive probe is sent. On Unix/Linux systems, it uses setsockopt() with the appropriate TCP keepalive option. On Windows, it delegates to pq_setkeepaliveswin32(). The function handles value validation, retrieves system defaults when needed, and gracefully handles platforms that don't support keepalive configuration. A value of 0 uses the system default.

## Parameters / Member Variables
- `idle`: Idle timeout value in seconds (0 = use system default)
- `port`: Pointer to Port structure containing socket and keepalive configuration

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getkeepalivesidle](pq_getkeepalivesidle.md) (to retrieve defaults)
  - [pq_setkeepaliveswin32](pq_setkeepaliveswin32.md) (Windows implementation)
  - setsockopt (system socket API, Unix/Linux only)
  - ereport (PostgreSQL logging)
  - STATUS_OK (PostgreSQL status constant)
  - STATUS_ERROR (PostgreSQL status constant)
  - PG_TCP_KEEPALIVE_IDLE (platform-specific constant)
  - PG_TCP_KEEPALIVE_IDLE_STR (string representation for logging)
- Called from (representative examples):
  - [pq_init](pq_init.md)
  - [assign_tcp_keepalives_idle](../a/assign_tcp_keepalives_idle.md)

## Notes and Other Information
- Platform-dependent implementation with conditional compilation
- Returns STATUS_OK immediately for Unix domain sockets (no-op)
- On Unix/Linux: uses setsockopt() with PG_TCP_KEEPALIVE_IDLE
- On Windows: delegates to pq_setkeepaliveswin32() with current interval
- Updates port->keepalives_idle on successful configuration
- Returns STATUS_ERROR on unsupported platforms when idle != 0
- Handles default value retrieval and validation logic
- Short-circuits if requested value matches current setting