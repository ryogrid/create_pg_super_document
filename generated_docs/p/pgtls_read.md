# pgtls_read

## Location
[src/interfaces/libpq/fe-secure-openssl.c:140-255](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L140-L255)

## Overview
Performs secure SSL/TLS data reading from a PostgreSQL connection with comprehensive error handling and OpenSSL error queue management.

## Definition

```c
ssize_t
pgtls_read(PGconn *conn, void *ptr, size_t len)
```
## Detailed Description
This function provides a secure wrapper around OpenSSL's SSL_read() function for reading data from an encrypted PostgreSQL connection. It implements robust error handling for various SSL error conditions and manages OpenSSL's per-thread error queue to ensure reliable operation in multi-threaded environments.

The function proactively clears the OpenSSL error queue before each read operation and properly retrieves errors afterward to prevent interference with other OpenSSL clients. It handles various SSL error conditions including connection closure, system errors, and SSL protocol errors, translating them into appropriate errno values and error messages.

For SSL_ERROR_WANT_WRITE conditions (when SSL needs to write during a read operation due to renegotiation), the function uses a busy-loop approach to avoid infinite blocking that could occur with incorrect wait conditions.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection object containing the SSL connection state
- `ptr`: Buffer to store the read data
- `len`: Maximum number of bytes to read

## Dependencies
- Functions called/Symbols referenced:
  - SOCK_ERRNO_SET
  - ERR_clear_error (OpenSSL)
  - SSL_read (OpenSSL)
  - SSL_get_error (OpenSSL)
  - ERR_get_error (OpenSSL)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - [SSLerrmessage](../S/SSLerrmessage.md)
  - [SSLerrfree](../S/SSLerrfree.md)
  - SOCK_STRERROR
  - Various errno constants (ECONNRESET, EPIPE)
- Called from (representative examples):
  - [pqsecure_read](pqsecure_read.md) (in fe-secure.c:189)
  - pgunlock_thread (referenced in libpq-int.h:819)

## Notes and Other Information
- Returns the number of bytes read on success, 0 for no data available, or -1 on error
- Sets appropriate errno values for different error conditions (ECONNRESET for connection issues)
- Includes special handling for SSL_ERROR_WANT_WRITE to prevent infinite waits
- Uses a goto loop (rloop) for SSL_ERROR_WANT_WRITE to busy-wait rather than blocking incorrectly
- Manages OpenSSL error queue defensively to maintain compatibility with other OpenSSL clients
- Distinguishes between clean connection closure (SSL_ERROR_ZERO_RETURN) and abnormal termination
- All error conditions result in detailed error messages being appended to conn->errorMessage
- Location: src/interfaces/libpq/fe-secure-openssl.c:140-255

## Simplified Source

```c
ssize_t pgtls_read(PGconn *conn, void *ptr, size_t len) {
    ssize_t bytes_read;
    int ssl_error;

rloop:
    // Clear OpenSSL error queue before operation
    SOCK_ERRNO_SET(0);
    ERR_clear_error();

    // Perform SSL read operation
    bytes_read = SSL_read(conn->ssl, ptr, len);
    ssl_error = SSL_get_error(conn->ssl, bytes_read);

    // Clean up error queue after operation
    unsigned long error_code = (ssl_error != SSL_ERROR_NONE || bytes_read < 0) ?
                              ERR_get_error() : 0;

    // Handle different SSL error conditions
    switch (ssl_error) {
        case SSL_ERROR_NONE:
            if (bytes_read < 0) {
                appendPQExpBufferStr(&conn->errorMessage,
                    "SSL_read failed but did not provide error information\n");
                SOCK_ERRNO_SET(ECONNRESET);
            }
            break;

        case SSL_ERROR_WANT_READ:
            bytes_read = 0;  // No data available yet
            break;

        case SSL_ERROR_WANT_WRITE:
            // SSL needs to write during read (renegotiation)
            // Busy-loop to avoid infinite wait
            goto rloop;

        case SSL_ERROR_SYSCALL:
            // System call error or connection closed
            if (bytes_read < 0 && SOCK_ERRNO != 0) {
                SOCK_ERRNO_SET(SOCK_ERRNO);
                if (SOCK_ERRNO == EPIPE || SOCK_ERRNO == ECONNRESET) {
                    libpq_append_conn_error(conn, "server closed the connection unexpectedly");
                } else {
                    libpq_append_conn_error(conn, "SSL SYSCALL error: %s",
                                          strerror(SOCK_ERRNO));
                }
            } else {
                libpq_append_conn_error(conn, "SSL SYSCALL error: EOF detected");
                SOCK_ERRNO_SET(ECONNRESET);
                bytes_read = -1;
            }
            break;

        case SSL_ERROR_SSL:
            // SSL protocol error
            libpq_append_conn_error(conn, "SSL error: %s", SSLerrmessage(error_code));
            SOCK_ERRNO_SET(ECONNRESET);
            bytes_read = -1;
            break;

        case SSL_ERROR_ZERO_RETURN:
            // Clean SSL connection closure
            libpq_append_conn_error(conn, "SSL connection has been closed unexpectedly");
            SOCK_ERRNO_SET(ECONNRESET);
            bytes_read = -1;
            break;

        default:
            // Unknown SSL error
            libpq_append_conn_error(conn, "unrecognized SSL error code: %d", ssl_error);
            SOCK_ERRNO_SET(ECONNRESET);
            bytes_read = -1;
            break;
    }

    return bytes_read;
}
```