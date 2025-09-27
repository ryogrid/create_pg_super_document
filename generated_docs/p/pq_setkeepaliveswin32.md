# pq_setkeepaliveswin32

## Location
[src/backend/libpq/pqcomm.c:1590-1628](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1590-L1628)

## Overview
Windows-specific function that configures TCP keepalive parameters on a socket using the WSAIoctl system call.

## Definition

```c
struct tcp_keepalive ka;
```
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

## Simplified Source

```c
// Simplified version of pq_setkeepaliveswin32
static int pq_setkeepaliveswin32(Port *port, int idle, int interval) {
    struct tcp_keepalive ka;
    DWORD retsize;

    // Apply default values if invalid parameters
    if (idle <= 0)
        idle = 2 * 60 * 60;    // default = 2 hours
    if (interval <= 0)
        interval = 1;          // default = 1 second

    // Configure keepalive structure
    ka.onoff = 1;
    ka.keepalivetime = idle * 1000;        // convert to milliseconds
    ka.keepaliveinterval = interval * 1000; // convert to milliseconds

    // Set keepalive parameters using Windows API
    if (WSAIoctl(port->sock, SIO_KEEPALIVE_VALS, &ka, sizeof(ka),
                 NULL, 0, &retsize, NULL, NULL) != 0) {
        // Log error and return failure
        ereport(LOG, (errmsg("WSAIoctl(SIO_KEEPALIVE_VALS) failed: error code %d",
                             WSAGetLastError())));
        return STATUS_ERROR;
    }

    // Update port settings on success
    if (port->keepalives_idle != idle)
        port->keepalives_idle = idle;
    if (port->keepalives_interval != interval)
        port->keepalives_interval = interval;

    return STATUS_OK;
}
```

Key simplifications made:
- Added explanatory comments for each logic block
- Simplified WSAIoctl parameter formatting
- Consolidated error message formatting
- Clarified unit conversions (seconds to milliseconds)
- Maintained essential Windows API and error handling logic