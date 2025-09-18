# assign_tcp_keepalives_idle

## Location
[src/backend/libpq/pqcomm.c:1951-1970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1951-L1970)

## Overview
A GUC (Grand Unified Configuration) assign hook function that applies changes to the tcp_keepalives_idle configuration parameter by setting the TCP keep-alive idle time for the current process's port connection.

## Definition
```c
void assign_tcp_keepalives_idle(int newval, void *extra)
```

## Detailed Description
This function serves as the assignment hook for the tcp_keepalives_idle GUC variable in PostgreSQL's configuration system. It is automatically called whenever the tcp_keepalives_idle parameter is changed via configuration files, SQL commands, or other GUC mechanisms. The function directly applies the new keep-alive idle time value to the current process's port connection using pq_setkeepalivesidle().

The implementation follows a simplified assignment strategy due to kernel API limitations. Rather than implementing the full check-then-assign GUC pattern, it immediately applies the setting and relies on the underlying pq_setkeepalivesidle() function to handle validation and error reporting. This approach acknowledges that the GUC value might not always match the actual kernel value, which is why PostgreSQL uses show_hook functions to retrieve the actual kernel values for display.

## Parameters / Member Variables
- `newval`: The new TCP keep-alive idle time value in seconds to be applied to the connection.
- `extra`: Additional data passed by the GUC system (unused in this implementation, hence cast to void to suppress compiler warnings).

## Dependencies
- Functions called/Symbols referenced:
  - [pq_setkeepalivesidle](../p/pq_setkeepalivesidle.md) (applies the new keep-alive idle setting to the port)
  - MyProcPort (global variable representing the current process's port)
- Called from (representative examples):
  - GUC system (automatically when tcp_keepalives_idle parameter changes)

## Notes and Other Information
- This is a GUC assign hook function, automatically invoked by PostgreSQL's configuration system
- The function intentionally uses a simplified assignment approach due to kernel API limitations
- Error handling is delegated to pq_setkeepalivesidle(), which reports issues via ereport(LOG)
- The GUC value may not reflect the actual kernel setting, requiring show_hook functions for accurate display
- Part of PostgreSQL's broader TCP keep-alive configuration system that includes idle time, interval, and count settings
- The function is declared in src/include/utils/guc_hooks.h and used by the GUC system infrastructure