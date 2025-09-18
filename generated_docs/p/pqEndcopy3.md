# pqEndcopy3

## Location
src/interfaces/libpq/fe-protocol3.c: 1916 - 2008

## Overview
Terminates a COPY operation in PostgreSQL protocol 3, handling the proper cleanup and message exchange required to end COPY IN, COPY OUT, or bidirectional COPY operations.

## Definition


## Detailed Description
pqEndcopy3 implements the protocol 3 termination sequence for PostgreSQL COPY operations. It validates the connection is in an appropriate COPY state, sends the necessary CopyDone message for COPY IN operations, handles extended-query mode synchronization, flushes pending data, and waits for the server's completion response. The function manages both blocking and non-blocking connection modes and provides backwards-compatible error handling by converting errors to notices.

The function performs a complete state transition from COPY mode back to PGASYNC_BUSY, ensuring proper cleanup of the COPY operation and readiness for subsequent operations.

## Parameters / Member Variables
- : PostgreSQL connection handle containing the connection state and message buffers

## Dependencies
- Functions called/Symbols referenced:
  - libpq_append_conn_error
  - pqPutMsgStart
  - pqPutMsgEnd
  - pqFlush
  - pqIsnonblocking
  - PQisBusy
  - PQgetResult
  - PQclear
  - pqInternalNotice
  - PGASYNC_COPY_IN (status constant)
  - PGASYNC_COPY_OUT (status constant)
  - PGASYNC_COPY_BOTH (status constant)
  - PGASYNC_BUSY (status constant)
  - PqMsg_CopyDone (message type)
  - PqMsg_Sync (message type)
  - PGQUERY_SIMPLE (query class constant)
  - PGRES_COMMAND_OK (result status)
- Called from (representative examples):
  - PQendcopy (in src/interfaces/libpq/fe-exec.c)

## Notes and Other Information
- Returns 0 on success, 1 on failure
- Sends CopyDone message only for COPY IN and bidirectional COPY operations
- Automatically sends Sync message when terminating extended-query mode COPY operations
- Handles both blocking and non-blocking connection modes appropriately
- For backwards compatibility, converts error messages to notices rather than returning them as errors
- Strips trailing newlines from error messages before converting to notices
- Part of the libpq protocol 3 implementation for PostgreSQL client-server communication