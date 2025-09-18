# pqGetlineAsync3

## Location
src/interfaces/libpq/fe-protocol3.c: 1861 - 1915

## Overview
Asynchronously retrieves COPY data rows without blocking, implementing the protocol 3 version of PostgreSQL's COPY OUT mechanism for libpq client library.

## Definition


## Detailed Description
pqGetlineAsync3 is the protocol 3 implementation of PostgreSQL's asynchronous COPY data retrieval mechanism. It reads COPY data from the connection's input buffer without blocking, making it suitable for non-blocking I/O operations. The function handles partial message consumption by tracking already-returned data in  to support scenarios where the caller's buffer is smaller than the available data.

The function validates the connection state to ensure it's in an appropriate COPY mode (COPY_OUT or COPY_BOTH), uses getCopyDataMessage to recognize and validate incoming messages, and manages buffer copying with support for partial reads when the caller's buffer is insufficient.

## Parameters / Member Variables
- : PostgreSQL connection handle containing the input buffer and connection state
- : Caller-provided buffer to receive the COPY data
- : Size of the caller's buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - getCopyDataMessage
  - PGASYNC_COPY_OUT (connection status constant)
  - PGASYNC_COPY_BOTH (connection status constant)
- Called from (representative examples):
  - PQgetlineAsync (in src/interfaces/libpq/fe-exec.c)

## Notes and Other Information
- Returns the number of bytes copied to the buffer, 0 if no data is available yet, or -1 for end-of-copy or error conditions
- Maintains state across calls using  to handle cases where the caller's buffer is smaller than the available message
- Does not change  unlike pqGetCopyData3, allowing PQendcopy to work without blocking
- Part of the libpq protocol 3 implementation for PostgreSQL client-server communication
- Designed for non-blocking I/O operations in asynchronous applications