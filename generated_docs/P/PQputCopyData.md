# PQputCopyData

## Location
src/interfaces/libpq/fe-exec.c: 2695 - 2748

## Overview
Sends data to the PostgreSQL backend during COPY IN or COPY BOTH operations, allowing clients to efficiently transfer bulk data to the server.

## Definition


## Detailed Description
PQputCopyData is a libpq function that transmits data to the PostgreSQL server during a COPY operation. It operates in both blocking and non-blocking modes, handling the low-level protocol details of sending copy data messages to the backend. The function validates the connection state, processes pending messages, manages output buffer space, and formats the data according to PostgreSQL's copy protocol.

The function performs several key operations:
- Validates that a COPY operation is currently in progress
- Processes any pending NOTICE or NOTIFY messages to prevent buffer overflow
- Manages output buffer space, flushing when necessary
- Wraps the user data in appropriate protocol messages (PqMsg_CopyData)
- Handles both blocking and non-blocking operation modes

## Parameters / Member Variables
- : PostgreSQL connection handle that must be in COPY IN or COPY BOTH state
- : Pointer to the data buffer to be sent to the server
- : Number of bytes to send from the buffer (must be > 0 for actual data transmission)

## Dependencies
- Functions called/Symbols referenced:
  - libpq_append_conn_error
  - parseInput
  - pqFlush
  - pqCheckOutBufferSpace
  - pqIsnonblocking
  - pqPutMsgStart
  - pqPutnchar
  - pqPutMsgEnd
  - PGASYNC_COPY_IN
  - PGASYNC_COPY_BOTH
  - PqMsg_CopyData
- Called from (representative examples):
  - handleCopyIn (psql)
  - ExecuteSqlCommandBuf (pg_dump)
  - sendFeedback (pg_basebackup tools)
  - libpqrcv_send (replication)

## Notes and Other Information
- Returns 1 on success, 0 if data couldn't be sent in non-blocking mode, or -1 on error
- The function assumes 5 bytes of protocol overhead when calculating buffer space requirements
- In non-blocking mode, the function may return 0 if insufficient buffer space is available
- The connection must be in PGASYNC_COPY_IN or PGASYNC_COPY_BOTH state before calling
- Processes pending messages during execution to prevent input buffer expansion
- Used primarily in COPY FROM STDIN operations and replication contexts