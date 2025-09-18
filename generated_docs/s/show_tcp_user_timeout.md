# show_tcp_user_timeout

## Location
src/backend/libpq/pqcomm.c: 2040 - 2052

## Overview
GUC show hook function that retrieves and displays the current TCP_USER_TIMEOUT value for the active database connection as a string.

## Definition
```c
const char *show_tcp_user_timeout(void)
```

## Detailed Description
This function serves as a GUC (Grand Unified Configuration) show hook for the `tcp_user_timeout` configuration parameter. It is automatically called by PostgreSQL's configuration system when the current value of tcp_user_timeout needs to be displayed (such as in response to SHOW commands or when querying pg_settings). The function retrieves the actual TCP_USER_TIMEOUT socket option value from the current connection and formats it as a string for display.

The function uses a static buffer to store the formatted string representation of the timeout value, which is returned to the GUC system for display purposes.

## Parameters / Member Variables
- No parameters (void function)
- Returns: String representation of the current tcp_user_timeout value in milliseconds

## Dependencies
- Functions called/Symbols referenced:
  - [pq_gettcpusertimeout](../p/pq_gettcpusertimeout.md)
  - MyProcPort (global variable representing the current connection port)
  - snprintf (standard C library function)
- Called from (representative examples):
  - GUC system infrastructure (referenced in guc_hooks.h)

## Notes and Other Information
- Uses a static 16-character buffer (nbuf) to store the formatted timeout value
- The function follows the same pattern as other GUC show hooks like those for TCP keepalive settings
- Returns "0" for Unix domain socket connections since TCP_USER_TIMEOUT doesn't apply to them
- The returned string remains valid until the next call to this function due to the static buffer
- Part of PostgreSQL's configuration display system that allows users to query current connection parameters
- On platforms without TCP_USER_TIMEOUT support, the underlying function returns 0