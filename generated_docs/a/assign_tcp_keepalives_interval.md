# assign_tcp_keepalives_interval

## Location
[src/backend/libpq/pqcomm.c:1984-1993](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1984-L1993)

## Overview
A GUC (Grand Unified Configuration) assign hook function that sets the TCP keepalive interval value for the current connection when the parameter is changed.

## Definition
```c
void assign_tcp_keepalives_interval(int newval, void *extra)
```

## Detailed Description
This function serves as the assignment hook for the PostgreSQL GUC parameter `tcp_keepalives_interval`. When a user changes this configuration parameter (e.g., via SET or ALTER SYSTEM), this function is automatically called to apply the new value to the current client connection. The function delegates the actual socket option setting to `pq_setkeepalivesinterval`, which handles the platform-specific implementation of setting the TCP_KEEPINTVL socket option.

The tcp_keepalives_interval parameter controls the time interval between individual keepalive probes once the keepalive mechanism has been activated on an idle connection.

## Parameters / Member Variables
- `newval`: The new integer value for the tcp_keepalives_interval parameter (in seconds)
- `extra`: Additional context data (unused in this function, part of GUC hook interface)

## Dependencies
- Functions called/Symbols referenced:
  - [pq_setkeepalivesinterval](../p/pq_setkeepalivesinterval.md): Sets the actual TCP keepalive interval socket option
  - `MyProcPort`: Global variable representing the current client connection port
- Called from (representative examples):
  - GUC system when parameter value is assigned or changed

## Notes and Other Information
- The function ignores the return value from `pq_setkeepalivesinterval`, following the same pattern as other keepalive assign hooks
- Platform-specific implementation details are handled by the underlying `pq_setkeepalivesinterval` function
- On Windows, uses SIO_KEEPALIVE_VALS; on Unix-like systems, uses TCP_KEEPINTVL socket option
- Part of PostgreSQL's libpq communication subsystem for managing client connections
- The actual socket option setting may fail on unsupported platforms, but the error is logged rather than propagated

## Simplified Source

```c
// Simplified version of assign_tcp_keepalives_interval
void assign_tcp_keepalives_interval(int newval, void *extra) {
    // Apply the new keepalive interval setting to current connection
    // See assign_tcp_keepalives_idle for detailed comments about error handling
    pq_setkeepalivesinterval(newval, MyProcPort);
}
```

Key simplifications made:
- Removed the void cast since the return value is intentionally ignored
- Added clear comment explaining the function's purpose
- Referenced the related idle function for detailed implementation notes
- Focused on the core functionality of applying TCP keepalive interval settings