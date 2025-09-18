# pq_setkeepaliveswin32

## Location
src/backend/libpq/pqcomm.c: 1590 - 1628

## Overview
Windows-specific function that configures TCP keepalive parameters on a socket using the WSAIoctl system call.

## Definition


## Detailed Description
This function implements TCP keepalive configuration specifically for Windows platforms. It uses the WSAIoctl function with SIO_KEEPALIVE_VALS to set keepalive parameters on a socket. The function enables keepalive probes and configures both the idle time (time before first keepalive probe) and interval (time between subsequent probes). Default values are applied if the provided parameters are invalid (<=0). Upon successful configuration, the function updates the port's keepalive state to reflect the new settings.

## Parameters / Member Variables
- `port`: Pointer to Port structure containing the socket descriptor and keepalive state
- `idle`: Time in seconds before sending first keepalive probe (default: 7200 seconds / 2 hours if <=0)
- `interval`: Time in seconds between keepalive probes (default: 1 second if <=0)

## Dependencies
- Functions called/Symbols referenced:
  - WSAIoctl (Windows socket API)
  - WSAGetLastError (Windows error reporting)
  - ereport (PostgreSQL logging)
  - STATUS_ERROR (PostgreSQL status constant)
  - STATUS_OK (PostgreSQL status constant)
- Called from (representative examples):
  - [pq_setkeepalivesidle](pq_setkeepalivesidle.md)
  - [pq_setkeepalivesinterval](pq_setkeepalivesinterval.md)

## Notes and Other Information
- Windows-specific implementation using struct tcp_keepalive and WSAIoctl
- Converts seconds to milliseconds for Windows API (multiplies by 1000)
- Only available on Windows platforms (#ifdef WIN32)
- Updates port->keepalives_idle and port->keepalives_interval on success
- Returns STATUS_ERROR on WSAIoctl failure, STATUS_OK on success