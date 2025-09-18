# pqSetKeepalivesWin32

## Location
src/interfaces/libpq/fe-connect.c: 2294 - 2322

## Overview
Enables keepalives and configures keepalive values on Windows systems using the Windows-specific WSAIoctl interface.

## Definition


## Detailed Description
This function provides Windows-specific implementation for setting TCP keepalive parameters on a socket. Unlike Unix systems where keepalive parameters are set individually using separate socket options, Windows requires all keepalive parameters to be set together in a single batch operation using the WSAIoctl system call with the SIO_KEEPALIVE_VALS control code.

The function enables keepalives and configures both the idle time (how long to wait before sending the first keepalive probe) and the interval time (time between subsequent keepalive probes). It applies reasonable defaults when invalid or zero values are provided: 2 hours for idle time and 1 second for interval time. The function is designed to be signal-safe since it's used by PQcancel operations.

## Parameters / Member Variables
- : The socket descriptor (pgsocket type) on which to configure keepalives
- : Time in seconds to wait before sending the first keepalive probe. Values <= 0 default to 7200 seconds (2 hours)
- : Time in seconds between successive keepalive probes. Values <= 0 default to 1 second

## Dependencies
- Functions called/Symbols referenced:
  - WSAIoctl (Windows socket I/O control function)
  - SIO_KEEPALIVE_VALS (Windows socket control code for keepalive configuration)
  - pgsocket (PostgreSQL socket type definition)
- Called from (representative examples):
  - PQcancel (connection cancellation functionality)
  - prepKeepalivesWin32 (keepalive preparation function)
  - ROOT_CRL_FILE (SSL certificate revocation list processing)

## Notes and Other Information
- Windows-specific function, only compiled and used on Windows platforms
- Must be signal-safe due to usage in PQcancel operations
- Converts time values from seconds to milliseconds for Windows API compatibility
- Returns 1 on success, 0 on failure
- Uses the tcp_keepalive structure to package all keepalive parameters together
- Provides sensible defaults for invalid input parameters to ensure robust operation
- Part of PostgreSQL's cross-platform socket management abstraction