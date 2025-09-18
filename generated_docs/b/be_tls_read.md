# be_tls_read

## Location
src/backend/libpq/be-secure-openssl.c: 761 - 819

## Overview
Reads encrypted data from an SSL/TLS connection, handling various SSL error conditions and providing appropriate wait indicators for non-blocking I/O operations.

## Definition


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
  - SSLerrmessage (error message formatting)
  - ereport (PostgreSQL error reporting)
  - COMMERROR (error level constant)
  - WL_SOCKET_READABLE / WL_SOCKET_WRITEABLE (wait event types)
  - EWOULDBLOCK / ECONNRESET (errno constants)

- Called from (representative examples):
  - secure_read (in be-secure.c:188)

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