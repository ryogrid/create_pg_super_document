# show_tcp_keepalives_count

## Location
[src/backend/libpq/pqcomm.c:2017-2029](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L2017-L2029)

## Overview
A GUC (Grand Unified Configuration) show hook function that displays the current TCP keepalive count value for the current connection.

## Definition
```c
const char *show_tcp_keepalives_count(void)
```

## Detailed Description
This function serves as the display hook for the PostgreSQL GUC parameter `tcp_keepalives_count`. It retrieves the current TCP keepalive count setting for the active client connection (MyProcPort) and formats it as a string for display. The function uses a static buffer to store the formatted integer value, which represents the maximum number of keepalive probes that will be sent before considering the connection dead.

The function is part of PostgreSQL's configuration parameter system and is called when the user queries the current value of the tcp_keepalives_count parameter (e.g., via `SHOW tcp_keepalives_count;`).

## Parameters / Member Variables
This function takes no parameters and operates on the global MyProcPort variable representing the current client connection.

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getkeepalivescount](../p/pq_getkeepalivescount.md): Retrieves the actual keepalive count value for the given port
  - `snprintf`: Formats the integer value into a string
  - `MyProcPort`: Global variable representing the current client connection port
- Called from (representative examples):
  - GUC system when displaying parameter values

## Notes and Other Information
- Uses a static 16-character buffer to store the formatted result
- The actual keepalive logic is platform-dependent and handled by `pq_getkeepalivescount`
- Returns "0" when keepalives are not supported or not configured
- The count value determines how many consecutive keepalive probes can fail before the connection is considered dead
- Part of PostgreSQL's libpq communication subsystem for managing client connections
- Works in conjunction with tcp_keepalives_idle and tcp_keepalives_interval to provide complete TCP keepalive functionality
- Only supported on platforms that provide the TCP_KEEPCNT socket option