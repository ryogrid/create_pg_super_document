# show_tcp_keepalives_interval

## Location
[src/backend/libpq/pqcomm.c:1994-2006](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1994-L2006)

## Overview
A GUC (Grand Unified Configuration) show hook function that displays the current TCP keepalive interval value for the current connection.

## Definition
```c
const char *show_tcp_keepalives_interval(void)
```

## Detailed Description
This function serves as the display hook for the PostgreSQL GUC parameter `tcp_keepalives_interval`. It retrieves the current TCP keepalive interval setting for the active client connection (MyProcPort) and formats it as a string for display. The function uses a static buffer to store the formatted integer value, which represents the number of seconds between individual keepalive probes once keepalive has been activated on an idle connection.

The function is part of PostgreSQL's configuration parameter system and is called when the user queries the current value of the tcp_keepalives_interval parameter (e.g., via `SHOW tcp_keepalives_interval;`).

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getkeepalivesinterval](../p/pq_getkeepalivesinterval.md): Retrieves the actual keepalive interval value for the given port
  - `snprintf`: Formats the integer value into a string
  - `MyProcPort`: Global variable representing the current client connection port
- Called from (representative examples):
  - GUC system when displaying parameter values

## Notes and Other Information
- Uses a static 16-character buffer to store the formatted result
- The actual keepalive logic is platform-dependent and handled by `pq_getkeepalivesinterval`
- Returns "0" when keepalives are not supported or not configured
- The interval value controls the time between successive keepalive probes (as opposed to the idle timeout before the first probe)
- Part of PostgreSQL's libpq communication subsystem for managing client connections
- Follows the same pattern as other TCP keepalive show hooks