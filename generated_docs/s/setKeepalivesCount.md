# setKeepalivesCount

## Location
src/interfaces/libpq/fe-connect.c: 2256 - 2293

## Overview
Sets the count of lost keepalive packets that will trigger a connection break for a PostgreSQL connection socket.

## Definition


## Detailed Description
This function configures the TCP keepalive count parameter for a PostgreSQL connection socket. The keepalive count determines how many consecutive keepalive probes can be lost before the TCP stack considers the connection dead and terminates it. The function parses the keepalive count value from the connection's  parameter, validates it, and applies it to the socket using the  socket option if available on the platform.

The function ensures that negative values are normalized to 0, and only attempts to set the socket option if the  option is supported by the system. If the  parameter is not set (NULL), the function returns success without making any changes.

## Parameters / Member Variables
- : Pointer to the PGconn structure representing the PostgreSQL connection. The function uses the  field from this structure and applies the setting to the connection's socket.

## Dependencies
- Functions called/Symbols referenced:
  - pqParseIntParam (parses integer parameter from string)
  - libpq_append_conn_error (appends error message to connection)
  - setsockopt (system call to set socket options)
  - SOCK_STRERROR (error string formatting macro)
  - SOCK_ERRNO (socket error number macro)
- Called from (representative examples):
  - CONNECTION_FAILED (connection establishment process)

## Notes and Other Information
- The function is conditionally compiled and only functional on systems that support the  socket option
- Negative keepalive count values are automatically converted to 0 to ensure valid configuration
- Returns 1 on success, 0 on failure
- Part of the PostgreSQL libpq connection establishment and configuration process
- Works in conjunction with other keepalive settings like keepalive interval and idle time to provide robust connection monitoring