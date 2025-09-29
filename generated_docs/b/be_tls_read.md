# be_tls_read

## Location
[src/backend/libpq/be-secure-openssl.c:761-819](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L761-L819)

## Overview
Reads encrypted data from an SSL/TLS connection, handling various SSL error conditions and providing appropriate wait indicators for non-blocking I/O operations.

## Definition

```c
ssize_t
be_tls_read(Port *port, void *ptr, size_t len, int *waitfor)
```
## Detailed Description
The  function provides a secure wrapper around OpenSSL's SSL_read() function for reading data from an established SSL/TLS connection. It handles the complexity of SSL error conditions and translates them into appropriate system-level errno values and wait conditions for PostgreSQL's I/O infrastructure.

The function performs the following operations:
1. **SSL Read Operation**: Calls SSL_read() to decrypt and read data from the SSL connection
2. **Error Analysis**: Uses SSL_get_error() to determine the specific type of SSL error that occurred
3. **Error Translation**: Maps SSL error codes to appropriate system errno values and wait conditions
4. **Non-blocking I/O Support**: Provides waitfor indicators for SSL_ERROR_WANT_READ and SSL_ERROR_WANT_WRITE conditions
5. **Error Reporting**: Reports SSL protocol violations and unrecognized errors using PostgreSQL's error reporting system

The function properly manages OpenSSL's per-thread error queue and provides comprehensive error handling for all SSL read scenarios.

## Parameters / Member Variables
- : Pointer to the Port structure containing the active SSL connection
- : Buffer to store the read data
- : Maximum number of bytes to read
- : Output parameter indicating what type of I/O event to wait for on EWOULDBLOCK (WL_SOCKET_READABLE or WL_SOCKET_WRITEABLE)

## Dependencies
- Functions called/Symbols referenced:
  - SSL_read (OpenSSL data read function)
  - SSL_get_error (SSL error analysis)
  - ERR_get_error / ERR_clear_error (OpenSSL error queue management)
  - [SSLerrmessage](../S/SSLerrmessage.md) (error message formatting)
  - ereport (PostgreSQL error reporting)
  - COMMERROR (error level constant)
  - WL_SOCKET_READABLE / WL_SOCKET_WRITEABLE (wait event types)
  - EWOULDBLOCK / ECONNRESET (errno constants)

- Called from (representative examples):
  - [secure_read](../s/secure_read.md) (in be-secure.c:188)

## Notes and Other Information
- Returns the number of bytes read on success, 0 on clean connection shutdown, or -1 on error
- The function sets errno appropriately for different error conditions:
  - EWOULDBLOCK for SSL_ERROR_WANT_READ/WRITE (non-blocking I/O)
  - ECONNRESET for SSL_ERROR_SYSCALL, SSL_ERROR_SSL, and unrecognized errors
- For SSL_ERROR_WANT_READ/WRITE, the caller should wait for the indicated socket condition before retrying
- SSL_ERROR_ZERO_RETURN indicates the peer has cleanly shut down the connection (returns 0)
- SSL_ERROR_SYSCALL is handled carefully - if no system error occurred, it's treated as a connection reset
- The function properly clears the OpenSSL error queue before operations and retrieves error codes when needed
- Error reporting uses COMMERROR level to indicate communication errors
- The waitfor parameter is only set for WANT_READ/WANT_WRITE conditions where the caller needs to wait
- This function is designed to integrate with PostgreSQL's non-blocking I/O infrastructure
- The function follows OpenSSL best practices for error queue management to ensure reliable operation

## Simplified Source

```c
ssize_t be_tls_read(Port *port, void *ptr, size_t len, int *waitfor)
{
    ssize_t n;
    int err;
    unsigned long ecode;

    // Clear OpenSSL error state and attempt to read
    errno = 0;
    ERR_clear_error();
    n = SSL_read(port->ssl, ptr, len);
    err = SSL_get_error(port->ssl, n);
    ecode = (err != SSL_ERROR_NONE || n < 0) ? ERR_get_error() : 0;

    // Handle different SSL error conditions
    switch (err)
    {
        case SSL_ERROR_NONE:
            // Successful read - return number of bytes read
            break;

        case SSL_ERROR_WANT_READ:
            // SSL needs more input data - tell caller to wait for socket readable
            *waitfor = WL_SOCKET_READABLE;
            errno = EWOULDBLOCK;
            n = -1;
            break;

        case SSL_ERROR_WANT_WRITE:
            // SSL needs to write data first - tell caller to wait for socket writable
            *waitfor = WL_SOCKET_WRITEABLE;
            errno = EWOULDBLOCK;
            n = -1;
            break;

        case SSL_ERROR_SYSCALL:
            // System call error - check if errno is meaningful
            if (n != -1 || errno == 0)
            {
                errno = ECONNRESET;
                n = -1;
            }
            break;

        case SSL_ERROR_SSL:
            // SSL protocol error - report and set connection reset
            ereport(COMMERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                     errmsg("SSL error: %s", SSLerrmessage(ecode))));
            errno = ECONNRESET;
            n = -1;
            break;

        case SSL_ERROR_ZERO_RETURN:
            // Clean connection shutdown by peer
            n = 0;
            break;

        default:
            // Unrecognized SSL error
            ereport(COMMERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                     errmsg("unrecognized SSL error code: %d", err)));
            errno = ECONNRESET;
            n = -1;
            break;
    }

    return n;
}
```