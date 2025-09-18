# assign_tcp_keepalives_count

## Location
[src/backend/libpq/pqcomm.c:2007-2016](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L2007-L2016)

## Overview
A GUC (Grand Unified Configuration) assign hook function that sets the TCP keepalive count value for the current connection when the parameter is changed.

## Definition
```c
void assign_tcp_keepalives_count(int newval, void *extra)
```

## Detailed Description
This function serves as the assignment hook for the PostgreSQL GUC parameter `tcp_keepalives_count`. When a user changes this configuration parameter (e.g., via SET or ALTER SYSTEM), this function is automatically called to apply the new value to the current client connection. The function delegates the actual socket option setting to `pq_setkeepalivescount`, which handles the platform-specific implementation of setting the TCP_KEEPCNT socket option.

The tcp_keepalives_count parameter controls the maximum number of keepalive probes that will be sent before considering the connection dead and closing it.

## Parameters / Member Variables
- `newval`: The new integer value for the tcp_keepalives_count parameter (number of probes)
- `extra`: Additional context data (unused in this function, part of GUC hook interface)

## Dependencies
- Functions called/Symbols referenced:
  - [pq_setkeepalivescount](../p/pq_setkeepalivescount.md): Sets the actual TCP keepalive count socket option
  - `MyProcPort`: Global variable representing the current client connection port
- Called from (representative examples):
  - GUC system when parameter value is assigned or changed

## Notes and Other Information
- The function ignores the return value from `pq_setkeepalivescount`, following the same pattern as other keepalive assign hooks
- Platform-specific implementation details are handled by the underlying `pq_setkeepalivescount` function
- Uses TCP_KEEPCNT socket option on platforms that support it
- Part of PostgreSQL's libpq communication subsystem for managing client connections
- The actual socket option setting may fail on unsupported platforms, but the error is logged rather than propagated
- This parameter works in conjunction with tcp_keepalives_idle and tcp_keepalives_interval to provide comprehensive TCP keepalive control