# setKeepalivesInterval

## Location
src/interfaces/libpq/fe-connect.c: 2221 - 2255

## Overview
Configures the TCP keepalive probe interval by parsing the connection parameter and setting the appropriate socket option on platforms that support TCP_KEEPINTVL.

## Definition
```c
static int setKeepalivesInterval(PGconn *conn)
```

## Detailed Description
This function manages the TCP keepalive interval configuration, which determines the time between successive keepalive probes after the initial idle period expires. The function:

1. **Parameter validation**: Parses the `keepalives_interval` connection parameter using libpq's standard integer parsing
2. **Value normalization**: Ensures negative values are converted to 0 (immediate successive probes)
3. **Platform support**: Only applies the setting on platforms that define `TCP_KEEPINTVL` (standard TCP socket option)
4. **Socket configuration**: Uses `setsockopt()` with `IPPROTO_TCP` and `TCP_KEEPINTVL` to configure probe intervals
5. **Error handling**: Reports detailed error messages if the socket option configuration fails

This works together with the keepalive idle timer to provide comprehensive dead connection detection.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn connection object containing the keepalives_interval parameter and socket descriptor

## Return Value
- **1**: Success (parameter applied successfully or skipped if NULL)
- **0**: Failure (parameter parsing error or setsockopt system call failure)

## Dependencies
- Functions called/Symbols referenced:
  - [pqParseIntParam](../p/pqParseIntParam.md) (for parsing integer connection parameters)
  - TCP_KEEPINTVL (standard TCP socket option constant)
  - SOCK_ERRNO, SOCK_STRERROR (error handling macros)
  - PG_STRERROR_R_BUFLEN (buffer size for error messages)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (for error message reporting)
- Called from (representative examples):
  - CONNECTION_FAILED (during connection setup and socket configuration)

## Notes and Other Information
- This is a static function internal to fe-connect.c
- Platform-dependent functionality: only active when `TCP_KEEPINTVL` is defined (most modern systems)
- Part of libpq's comprehensive TCP keepalive configuration system
- Negative values are automatically converted to 0 for safety
- If keepalives_interval parameter is NULL, the function succeeds without action (using system defaults)
- Works in conjunction with `setKeepalivesIdle` to provide complete keepalive timing control
- The interval applies to subsequent probes after the initial idle period expires