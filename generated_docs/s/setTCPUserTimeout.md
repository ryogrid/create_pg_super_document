# setTCPUserTimeout

## Location
[src/interfaces/libpq/fe-connect.c:2353-2391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L2353-L2391)

## Overview
Configures the TCP user timeout parameter for a PostgreSQL connection socket, controlling the maximum time for unacknowledged data transmission.

## Definition

```c
static int
setTCPUserTimeout(PGconn *conn)
```
## Detailed Description
This function sets the TCP user timeout parameter on a PostgreSQL connection socket using the TCP_USER_TIMEOUT socket option. The TCP user timeout specifies the maximum amount of time that transmitted data may remain unacknowledged before the TCP stack considers the connection broken. This timeout encompasses both the time for retransmissions and the time waiting for acknowledgments.

The function parses the timeout value from the connection's  parameter, validates it (converting negative values to 0), and applies it to the socket. If the parameter is not set (NULL), the function returns success without making changes. The functionality is conditionally available only on systems that support the TCP_USER_TIMEOUT socket option.

## Parameters / Member Variables
- `*conn`: Pointer to the PGconn structure representing the PostgreSQL connection. The function uses the  field from this structure and applies the setting to the connection's socket.
## Dependencies
- Functions called/Symbols referenced:
  - [pqParseIntParam](../p/pqParseIntParam.md) (parses integer parameter from string)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (appends error message to connection)
  - setsockopt (system call to set socket options)
  - SOCK_STRERROR (error string formatting macro)
  - SOCK_ERRNO (socket error number macro)
- Called from (representative examples):
  - CONNECTION_FAILED (connection establishment process)

## Notes and Other Information
- Only functional on systems that support the TCP_USER_TIMEOUT socket option (primarily Linux)
- Negative timeout values are automatically converted to 0 to ensure valid configuration
- Returns 1 on success, 0 on failure with detailed error reporting
- Part of PostgreSQL's advanced TCP tuning capabilities for connection reliability
- The timeout value is specified in milliseconds
- Complements other TCP settings like keepalive parameters for comprehensive connection monitoring
- Useful for detecting network failures more quickly than relying solely on keepalive mechanisms

## Simplified Source

```c
static int setTCPUserTimeout(PGconn *conn) {
    int timeout;

    // Skip if not configured
    if (conn->pgtcp_user_timeout == NULL)
        return 1;

    // Parse timeout parameter
    if (!pqParseIntParam(conn->pgtcp_user_timeout, &timeout, conn, "tcp_user_timeout"))
        return 0;

    // Ensure non-negative value
    if (timeout < 0)
        timeout = 0;

#ifdef TCP_USER_TIMEOUT
    // Apply timeout setting to socket
    if (setsockopt(conn->sock, IPPROTO_TCP, TCP_USER_TIMEOUT,
                   (char *) &timeout, sizeof(timeout)) < 0) {
        char errBuf[256];
        libpq_append_conn_error(conn, "setsockopt(TCP_USER_TIMEOUT) failed: %s",
                                SOCK_STRERROR(SOCK_ERRNO, errBuf, sizeof(errBuf)));
        return 0;
    }
#endif

    return 1;
}
```