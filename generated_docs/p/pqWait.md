# pqWait

## Location
src/interfaces/libpq/fe-misc.c: 1004 - 1019

## Overview
pqWait is a PostgreSQL libpq function that waits until the connection socket becomes ready for reading or writing operations.

## Definition


## Detailed Description
pqWait is a convenience wrapper function that provides socket waiting functionality for PostgreSQL client connections. It delegates to pqWaitTimed with an infinite timeout (-1), meaning it will wait indefinitely until the socket becomes ready for the requested operation. The function handles both read and write readiness conditions and also monitors for exception conditions on the socket. When SSL is enabled and the operation is for reading, any buffered bytes will short-circuit the need to wait on the socket.

## Parameters / Member Variables
- : Integer flag indicating whether to wait for read readiness (non-zero means wait for read)
- : Integer flag indicating whether to wait for write readiness (non-zero means wait for write)  
- : Pointer to the PGconn connection structure representing the database connection

## Dependencies
- Functions called/Symbols referenced:
  - pqWaitTimed
- Called from (representative examples):
  - PQgetResult
  - pqSendSome
  - pqGetCopyData3
  - pqGetline3
  - pqFunctionCall3

## Notes and Other Information
- This function is a simple wrapper around pqWaitTimed with an infinite timeout
- When SSL is enabled and forRead is true, buffered bytes can bypass the socket wait
- Exception conditions on the socket will cause the function to return, with the actual error detected on subsequent read/write attempts
- The function is primarily used internally by libpq for managing connection I/O operations
- File location: src/interfaces/libpq/fe-misc.c:1004-1019