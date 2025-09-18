# be_tls_write

## Location
src/backend/libpq/be-secure-openssl.c: 820 - 907

## Overview
The  function provides secure data transmission over TLS/SSL connections in PostgreSQL's backend, handling SSL write operations with appropriate error handling and wait state management.

## Definition


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