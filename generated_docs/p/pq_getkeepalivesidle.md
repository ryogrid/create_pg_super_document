# pq_getkeepalivesidle

## Location
[src/backend/libpq/pqcomm.c:1629-1663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1629-L1663)

## Overview
Retrieves the current TCP keepalive idle timeout value for a given port connection.

## Definition


## Detailed Description
This function returns the TCP keepalive idle time setting for a port connection. It first checks if a custom keepalive idle time has been set on the port. If not, it attempts to retrieve the system default value using getsockopt() on non-Windows systems. On Windows platforms, the default values cannot be retrieved, so the function returns -1 to indicate "don't know". The function handles both Unix domain sockets (returns 0) and TCP sockets with appropriate platform-specific logic.

## Parameters / Member Variables
- `port`: Pointer to Port structure containing socket and keepalive configuration

## Dependencies
- Functions called/Symbols referenced:
  - getsockopt (system socket API, Unix/Linux only)
  - ereport (PostgreSQL logging)
  - PG_TCP_KEEPALIVE_IDLE (platform-specific TCP keepalive idle constant)
  - PG_TCP_KEEPALIVE_IDLE_STR (string representation for logging)
  - AF_UNIX (address family constant)
  - IPPROTO_TCP (protocol constant)
- Called from (representative examples):
  - [pq_setkeepalivesidle](pq_setkeepalivesidle.md)
  - [show_tcp_keepalives_idle](../s/show_tcp_keepalives_idle.md)

## Notes and Other Information
- Platform-dependent implementation using conditional compilation (#ifdef PG_TCP_KEEPALIVE_IDLE)
- Returns 0 for Unix domain sockets or when TCP keepalive is not supported
- Returns port->keepalives_idle if explicitly set
- Returns port->default_keepalives_idle or retrieves it via getsockopt() on first call
- Windows version always returns -1 for default values (cannot query defaults)
- Caches default value in port->default_keepalives_idle to avoid repeated system calls