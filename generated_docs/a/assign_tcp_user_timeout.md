# assign_tcp_user_timeout

## Location
[src/backend/libpq/pqcomm.c:2030-2039](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L2030-L2039)

## Overview
GUC assign hook function that sets the TCP_USER_TIMEOUT socket option on the current connection when the tcp_user_timeout GUC parameter is changed.

## Definition


## Detailed Description
This function serves as a GUC (Grand Unified Configuration) assign hook for the  configuration parameter. When PostgreSQL's configuration system updates the tcp_user_timeout setting, this function is automatically called to apply the new timeout value to the active database connection. The TCP_USER_TIMEOUT socket option specifies the maximum amount of time that transmitted data may remain unacknowledged before the TCP connection is forcibly closed.

The function acts as a thin wrapper around , delegating the actual socket option setting to that lower-level function while providing the GUC system interface.

## Parameters / Member Variables
- : The new tcp_user_timeout value in milliseconds to be applied to the connection
- : Additional data from the GUC system (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [pq_settcpusertimeout](../p/pq_settcpusertimeout.md)
  - MyProcPort (global variable representing the current connection port)
- Called from (representative examples):
  - GUC system infrastructure (referenced in guc_hooks.h)

## Notes and Other Information
- This function follows the same pattern as , as indicated by the code comment
- The function ignores the  parameter by explicitly casting it to void
- Only applies to TCP connections; Unix domain socket connections are handled appropriately by the underlying  function
- Part of PostgreSQL's configuration management system that allows runtime changes to connection parameters
- The TCP_USER_TIMEOUT feature may not be available on all platforms, which is handled by the underlying implementation