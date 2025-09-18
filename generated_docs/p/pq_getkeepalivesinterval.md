# pq_getkeepalivesinterval

## Location
src/backend/libpq/pqcomm.c: 1714 - 1748

## Overview
Retrieves the current TCP keepalive interval value (time between keepalive probes) for a given port connection.

## Definition


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