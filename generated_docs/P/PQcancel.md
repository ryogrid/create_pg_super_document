# PQcancel

## Location
src/interfaces/libpq/fe-cancel.c: 464 - 661

## Overview
Sends a cancel request to the PostgreSQL backend to terminate a currently executing query, using an old, non-encrypted but signal-safe protocol.

## Definition


## Detailed Description
PQcancel implements the original PostgreSQL query cancellation mechanism. It establishes a temporary TCP connection to the PostgreSQL server and sends a cancel request packet containing the backend process ID and authentication key. The function is designed to be signal-safe, making it suitable for use in signal handlers (e.g., SIGINT). It uses only reentrant system calls and avoids malloc/free operations. The function sets up keepalive options on the socket to prevent indefinite blocking, sends the cancellation request, and waits for the server to close the connection as confirmation of receipt.

## Parameters / Member Variables
- : Pointer to PGcancel structure containing connection details, backend PID, and authentication key
- : Buffer to store error messages on failure (recommended size 256 bytes)
- : Size of the error buffer

## Dependencies
- Functions called/Symbols referenced:
  - socket (system call for creating socket)
  - connect (system call for establishing connection)
  - send (system call for sending data)
  - recv (system call for receiving data)
  - closesocket (socket cleanup)
  - optional_setsockopt (helper for socket options)
  - strlcpy (safe string copying)
  - pg_hton32 (host to network byte order conversion)
  - pqSetKeepalivesWin32 (Windows keepalive configuration)
- Called from (representative examples):
  - ShutdownWorkersHard (src/bin/pg_dump/parallel.c:433)
  - sigTermHandler (src/bin/pg_dump/parallel.c:581)
  - handle_sigint (src/fe_utils/cancel.c:165)
  - PQrequestCancel (src/interfaces/libpq/fe-cancel.c:685)
  - test_cancel (src/test/modules/libpq_pipeline/libpq_pipeline.c:267)

## Notes and Other Information
- Signal-safe implementation suitable for use in signal handlers
- Returns true on successful dispatch, false on failure (does not guarantee query cancellation)
- Uses only reentrant functions to avoid reentrancy issues
- Configures TCP keepalive options to prevent indefinite blocking
- Implements retry logic for interrupted system calls (EINTR)
- Creates temporary socket connection specifically for the cancel request
- Error messages are built using safe string operations without sprintf
- Part of the legacy cancellation API, with newer encrypted alternatives available
- Location: src/interfaces/libpq/fe-cancel.c:464-661