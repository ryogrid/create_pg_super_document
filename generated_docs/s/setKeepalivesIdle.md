# setKeepalivesIdle

## Location
[src/interfaces/libpq/fe-connect.c:2187-2220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L2187-L2220)

## Overview
Configures the TCP keepalive idle timer by parsing the connection parameter and setting the appropriate socket option on platforms that support it.

## Definition
```c
static int setKeepalivesIdle(PGconn *conn)
```

## Detailed Description
This function manages the TCP keepalive idle time configuration, which determines how long a connection must be idle before the first keepalive probe is sent. The function:

1. **Parameter validation**: Parses the `keepalives_idle` connection parameter using libpq's standard integer parsing
2. **Value normalization**: Ensures negative values are converted to 0 (immediate keepalive)
3. **Platform support**: Only applies the setting on platforms that define `PG_TCP_KEEPALIVE_IDLE`
4. **Socket configuration**: Uses `setsockopt()` with `IPPROTO_TCP` to configure the idle timer
5. **Error handling**: Reports detailed error messages if the socket option fails

The keepalive idle timer is a critical component of TCP's dead connection detection mechanism.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn connection object containing the keepalives_idle parameter and socket descriptor

## Return Value
- **1**: Success (parameter applied or skipped if NULL)
- **0**: Failure (parameter parsing error or setsockopt failure)

## Dependencies
- Functions called/Symbols referenced:
  - [pqParseIntParam](../p/pqParseIntParam.md) (for parsing integer connection parameters)
  - PG_TCP_KEEPALIVE_IDLE (platform-specific socket option constant)
  - PG_TCP_KEEPALIVE_IDLE_STR (string representation for error messages)
  - SOCK_ERRNO, SOCK_STRERROR (error handling macros)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (for error message reporting)
- Called from (representative examples):
  - CONNECTION_FAILED (during connection setup and socket configuration)

## Notes and Other Information
- This is a static function internal to fe-connect.c
- Platform-dependent functionality: only active when `PG_TCP_KEEPALIVE_IDLE` is defined
- Part of libpq's comprehensive TCP keepalive configuration system
- Negative values are automatically converted to 0 for safety
- If keepalives_idle parameter is NULL, the function succeeds without action (using system defaults)
- Works in conjunction with other keepalive functions like `setKeepalivesInterval`