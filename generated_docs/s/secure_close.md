# secure_close

## Location
src/backend/libpq/be-secure.c: 163 - 174

## Overview
Closes a secure SSL/TLS session and cleans up SSL-related resources for a client connection.

## Definition


## Detailed Description
The `secure_close` function handles the proper termination of SSL/TLS connections by checking if the connection is actually using SSL and then delegating to the underlying SSL implementation for cleanup. It serves as a safe wrapper that only performs SSL closure operations when SSL is both compiled in and actively being used for the connection.

The function checks the `ssl_in_use` flag in the Port structure to determine if SSL is active before calling `be_tls_close` to perform the actual SSL session termination and resource cleanup. This conditional approach ensures that the function can be safely called on any Port without causing errors for non-SSL connections.

## Parameters / Member Variables
- `port`: Pointer to a Port structure representing the client connection. The function checks the `ssl_in_use` field to determine if SSL cleanup is needed.

## Dependencies
- Functions called/Symbols referenced:
  - [be_tls_close](../b/be_tls_close.md) (performs actual SSL session termination)
  - [Port](../P/Port.md) (connection structure type)
  - USE_SSL (compile-time macro check)
- Called from (representative examples):
  - [socket_close](socket_close.md) (as part of general connection cleanup)
  - FeBeWaitSetNEvents (referenced in libpq.h)

## Notes and Other Information
- This function has no return value (void) as it performs cleanup operations
- Safe to call on any Port, regardless of whether SSL is in use
- Only performs SSL cleanup when both SSL support is compiled in AND the connection is using SSL
- Part of PostgreSQL's connection lifecycle management
- Called during normal connection termination and error cleanup scenarios
- The function is designed to be safe and non-destructive for non-SSL connections