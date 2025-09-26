# info_cb

## Location
src/backend/libpq/be-secure-openssl.c: 1272 - 1322

## Overview
A static callback function that logs SSL connection state information and events to the PostgreSQL server log for debugging purposes.

## Definition


## Detailed Description
The  function serves as an OpenSSL information callback that captures and logs various SSL/TLS connection events and state transitions. It is registered with OpenSSL to provide detailed debugging information about SSL handshake processes, connection states, and alert conditions. The function uses PostgreSQL's  mechanism to log messages at DEBUG4 level, making SSL connection diagnostics available through the standard PostgreSQL logging system.

The callback handles multiple SSL event types including handshake start/completion, accept/connect loop states, and SSL alert conditions. Each event type is logged with descriptive text obtained from OpenSSL's state description functions.

## Parameters / Member Variables
- : Pointer to the SSL connection structure providing context about the current SSL session
- : Integer flag indicating the type of SSL event or state change being reported
- : Additional arguments or status codes associated with the SSL event (used for exit codes and alert details)

## Dependencies
- Functions called/Symbols referenced:
  - SSL_state_string_long (OpenSSL function)
  - ereport (PostgreSQL logging function)
  - errmsg_internal (PostgreSQL error message formatting)
  - DEBUG4 (PostgreSQL log level constant)
- Called from (representative examples):
  - be_tls_open_server (registered as SSL info callback)

## Notes and Other Information
- This function is only active when PostgreSQL is compiled with OpenSSL support
- Messages are logged at DEBUG4 level, requiring appropriate log_min_messages configuration to be visible
- The function handles 8 different SSL event types: handshake start/done, accept loop/exit, connect loop/exit, and read/write alerts
- Alert messages include hexadecimal codes for detailed SSL protocol diagnostics
- This callback provides essential debugging information for SSL/TLS connection troubleshooting in PostgreSQL