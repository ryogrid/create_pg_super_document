# pq_setkeepalivesinterval

## Location
src/backend/libpq/pqcomm.c: 1749 - 1797

## Overview
Sets the TCP keepalive interval value (time between keepalive probes) for a given port connection using platform-specific implementations.

## Definition


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