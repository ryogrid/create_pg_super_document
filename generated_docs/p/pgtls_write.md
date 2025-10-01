# pgtls_write

## Location
[src/interfaces/libpq/fe-secure-openssl.c:262-361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L262-L361)

## Overview
Performs secure SSL/TLS data writing to a PostgreSQL connection with comprehensive error handling and OpenSSL error queue management.

## Definition

```c
ssize_t
pgtls_write(PGconn *conn, const void *ptr, size_t len)
```
## Detailed Description
This function provides a secure wrapper around OpenSSL's SSL_write() function for writing data to an encrypted PostgreSQL connection. Similar to pgtls_read(), it implements robust error handling for various SSL error conditions and manages OpenSSL's per-thread error queue to ensure reliable operation in multi-threaded environments.

The function follows the same error queue management pattern as pgtls_read(), clearing the OpenSSL error queue before the write operation and properly retrieving errors afterward. It handles various SSL error conditions including partial writes, connection closure, system errors, and SSL protocol errors, translating them into appropriate errno values and error messages.

For SSL_ERROR_WANT_READ conditions (when SSL needs to read during a write operation due to renegotiation), the function returns 0 to indicate that the caller should wait, though this may not be the ideal behavior as noted in the code comments.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection object containing the SSL connection state
- `ptr`: Buffer containing the data to write
- `len`: Number of bytes to write from the buffer

## Dependencies
- Functions called/Symbols referenced:
  - SOCK_ERRNO_SET
  - ERR_clear_error (OpenSSL)
  - SSL_write (OpenSSL)
  - SSL_get_error (OpenSSL)
  - ERR_get_error (OpenSSL)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - [SSLerrmessage](../S/SSLerrmessage.md)
  - [SSLerrfree](../S/SSLerrfree.md)
  - SOCK_STRERROR
  - Various errno constants (ECONNRESET, EPIPE)
- Called from (representative examples):
  - [pqsecure_write](pqsecure_write.md) (in fe-secure.c:289)
  - pgunlock_thread (referenced in libpq-int.h:833)

## Notes and Other Information
- Returns the number of bytes written on success, 0 for no data written (retry needed), or -1 on error
- Sets appropriate errno values for different error conditions (ECONNRESET for connection issues)
- For SSL_ERROR_WANT_READ, returns 0 but notes this may not be optimal behavior for the caller
- Handles the fact that SSL_write can perform reads internally (noted in SSL_ERROR_SYSCALL handling)
- Manages OpenSSL error queue defensively to maintain compatibility with other OpenSSL clients
- Distinguishes between clean connection closure (SSL_ERROR_ZERO_RETURN) and abnormal termination
- All error conditions result in detailed error messages being appended to conn->errorMessage
- Part of the non-blocking I/O framework where partial writes are expected and handled
- Location: src/interfaces/libpq/fe-secure-openssl.c:262-361

## Simplified Source

```c
ssize_t pgtls_write(PGconn *conn, const void *ptr, size_t len)
{
    ssize_t n;
    int result_errno = 0;
    char sebuf[PG_STRERROR_R_BUFLEN];
    int err;
    unsigned long ecode;

    // Clear OpenSSL error state and perform write
    SOCK_ERRNO_SET(0);
    ERR_clear_error();
    n = SSL_write(conn->ssl, ptr, len);
    err = SSL_get_error(conn->ssl, n);
    ecode = (err != SSL_ERROR_NONE || n < 0) ? ERR_get_error() : 0;

    // Handle different SSL error conditions
    switch (err) {
        case SSL_ERROR_NONE:
            if (n < 0) {
                // Unexpected error condition
                appendPQExpBufferStr(&conn->errorMessage,
                                   "SSL_write failed but did not provide error information\n");
                result_errno = ECONNRESET;
            }
            break;

        case SSL_ERROR_WANT_READ:
            // SSL needs to read during write (renegotiation)
            // Return 0 to indicate retry needed (not ideal but best we can do)
            n = 0;
            break;

        case SSL_ERROR_WANT_WRITE:
            // Need to retry write operation
            n = 0;
            break;

        case SSL_ERROR_SYSCALL:
            // System call error or EOF
            if (n < 0 && SOCK_ERRNO != 0) {
                result_errno = SOCK_ERRNO;
                if (result_errno == EPIPE || result_errno == ECONNRESET)
                    libpq_append_conn_error(conn, "server closed the connection unexpectedly\n"
                                          "\tThis probably means the server terminated abnormally\n"
                                          "\tbefore or while processing the request.");
                else
                    libpq_append_conn_error(conn, "SSL SYSCALL error: %s",
                                          SOCK_STRERROR(result_errno, sebuf, sizeof(sebuf)));
            } else {
                // EOF detected
                libpq_append_conn_error(conn, "SSL SYSCALL error: EOF detected");
                result_errno = ECONNRESET;
                n = -1;
            }
            break;

        case SSL_ERROR_SSL:
            // SSL protocol error
            {
                char *errm = SSLerrmessage(ecode);
                libpq_append_conn_error(conn, "SSL error: %s", errm);
                SSLerrfree(errm);
                result_errno = ECONNRESET;
                n = -1;
                break;
            }

        case SSL_ERROR_ZERO_RETURN:
            // Clean connection closure
            libpq_append_conn_error(conn, "SSL connection has been closed unexpectedly");
            result_errno = ECONNRESET;
            n = -1;
            break;

        default:
            // Unrecognized error
            libpq_append_conn_error(conn, "unrecognized SSL error code: %d", err);
            result_errno = ECONNRESET;
            n = -1;
            break;
    }

    // Set errno for caller
    SOCK_ERRNO_SET(result_errno);

    return n;
}
```