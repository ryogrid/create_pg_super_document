# be_tls_write

## Location
[src/backend/libpq/be-secure-openssl.c:820-907](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L820-L907)

## Overview
The  function provides secure data transmission over TLS/SSL connections in PostgreSQL's backend, handling SSL write operations with appropriate error handling and wait state management.

## Definition

```c
ssize_t
be_tls_write(Port *port, void *ptr, size_t len, int *waitfor)
```
## Detailed Description
This function serves as the primary interface for writing data to an SSL-encrypted connection in PostgreSQL's backend. It wraps OpenSSL's  function and provides comprehensive error handling for various SSL error conditions. The function translates SSL-specific errors into standard POSIX errno values and manages non-blocking I/O scenarios by setting appropriate wait conditions.

The function handles multiple SSL error states including:
- Normal successful writes
- Non-blocking I/O situations requiring socket readability or writability
- System call errors and connection reset scenarios
- SSL protocol violations and connection closure events

## Parameters / Member Variables
- : Pointer to the Port structure containing the SSL connection state and socket information
- : Pointer to the data buffer to be written to the SSL connection
- : Number of bytes to write from the buffer
- : Output parameter that indicates what type of I/O wait is needed (WL_SOCKET_READABLE or WL_SOCKET_WRITEABLE)

## Dependencies
- Functions called/Symbols referenced:
  - SSL_write (OpenSSL function)
  - SSL_get_error (OpenSSL function)
  - ERR_get_error (OpenSSL function)
  - ERR_clear_error (OpenSSL function)
  - [SSLerrmessage](../S/SSLerrmessage.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - [secure_write](../s/secure_write.md)

## Notes and Other Information
- The function returns the number of bytes written on success, or -1 on error with errno set appropriately
- Non-blocking I/O is supported through the waitfor parameter, which indicates whether the caller should wait for socket readability or writability
- SSL_ERROR_SYSCALL handling includes special logic for EOF conditions when errno is zero
- All SSL protocol violations and unexpected errors are logged using PostgreSQL's error reporting system
- The function properly clears OpenSSL error state before operation and captures error codes for detailed reporting

## Simplified Source

```c
// Simplified version of be_tls_write
ssize_t be_tls_write(Port *port, void *ptr, size_t len, int *waitfor) {
    // Clear previous SSL errors and perform write
    errno = 0;
    ERR_clear_error();
    ssize_t bytes_written = SSL_write(port->ssl, ptr, len);
    int ssl_error = SSL_get_error(port->ssl, bytes_written);

    // Handle different SSL error conditions
    switch (ssl_error) {
        case SSL_ERROR_NONE:
            // Success - data written successfully
            break;

        case SSL_ERROR_WANT_READ:
            // SSL needs to read before writing can continue
            *waitfor = WL_SOCKET_READABLE;
            errno = EWOULDBLOCK;
            return -1;

        case SSL_ERROR_WANT_WRITE:
            // SSL needs to write before operation can continue
            *waitfor = WL_SOCKET_WRITEABLE;
            errno = EWOULDBLOCK;
            return -1;

        case SSL_ERROR_SYSCALL:
            // System call error or connection reset
            if (bytes_written != -1 || errno == 0) {
                errno = ECONNRESET;  // Treat as connection reset
            }
            return -1;

        case SSL_ERROR_SSL:
            // SSL protocol error
            ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                              errmsg("SSL error: %s", SSLerrmessage(ERR_get_error()))));
            errno = ECONNRESET;
            return -1;

        case SSL_ERROR_ZERO_RETURN:
            // SSL connection was cleanly closed
            errno = ECONNRESET;
            return -1;

        default:
            // Unknown SSL error
            ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                              errmsg("unrecognized SSL error code: %d", ssl_error)));
            errno = ECONNRESET;
            return -1;
    }

    return bytes_written;
}
```

Key simplifications made:
- Added descriptive comments for each error case
- Used more descriptive variable names (bytes_written, ssl_error)
- Consolidated error code retrieval logic
- Preserved all essential SSL error handling
- Maintained non-blocking I/O support through waitfor parameter