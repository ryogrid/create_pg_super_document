# pqEndcopy3

## Location
[src/interfaces/libpq/fe-protocol3.c:1916-2008](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L1916-L2008)

## Overview
Terminates a COPY operation in PostgreSQL protocol 3, handling the proper cleanup and message exchange required to end COPY IN, COPY OUT, or bidirectional COPY operations.

## Definition

```c
int
pqEndcopy3(PGconn *conn)
```
## Detailed Description
pqEndcopy3 implements the protocol 3 termination sequence for PostgreSQL COPY operations. It validates the connection is in an appropriate COPY state, sends the necessary CopyDone message for COPY IN operations, handles extended-query mode synchronization, flushes pending data, and waits for the server's completion response. The function manages both blocking and non-blocking connection modes and provides backwards-compatible error handling by converting errors to notices.

The function performs a complete state transition from COPY mode back to PGASYNC_BUSY, ensuring proper cleanup of the COPY operation and readiness for subsequent operations.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle containing the connection state and message buffers
## Dependencies
- Functions called/Symbols referenced:
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - [pqPutMsgStart](pqPutMsgStart.md)
  - [pqPutMsgEnd](pqPutMsgEnd.md)
  - [pqFlush](pqFlush.md)
  - pqIsnonblocking
  - [PQisBusy](../P/PQisBusy.md)
  - [PQgetResult](../P/PQgetResult.md)
  - [PQclear](../P/PQclear.md)
  - [pqInternalNotice](pqInternalNotice.md)
  - PGASYNC_COPY_IN (status constant)
  - PGASYNC_COPY_OUT (status constant)
  - PGASYNC_COPY_BOTH (status constant)
  - PGASYNC_BUSY (status constant)
  - PqMsg_CopyDone (message type)
  - PqMsg_Sync (message type)
  - PGQUERY_SIMPLE (query class constant)
  - PGRES_COMMAND_OK (result status)
- Called from (representative examples):
  - [PQendcopy](../P/PQendcopy.md) (in src/interfaces/libpq/fe-exec.c)

## Notes and Other Information
- Returns 0 on success, 1 on failure
- Sends CopyDone message only for COPY IN and bidirectional COPY operations
- Automatically sends Sync message when terminating extended-query mode COPY operations
- Handles both blocking and non-blocking connection modes appropriately
- For backwards compatibility, converts error messages to notices rather than returning them as errors
- Strips trailing newlines from error messages before converting to notices
- Part of the libpq protocol 3 implementation for PostgreSQL client-server communication

## Simplified Source

```c
int pqEndcopy3(PGconn *conn) {
    // Check if we're in a valid COPY state
    if (conn->asyncStatus != PGASYNC_COPY_IN &&
        conn->asyncStatus != PGASYNC_COPY_OUT &&
        conn->asyncStatus != PGASYNC_COPY_BOTH) {
        libpq_append_conn_error(conn, "no COPY in progress");
        return 1;
    }

    // Send CopyDone message for input operations
    if (conn->asyncStatus == PGASYNC_COPY_IN ||
        conn->asyncStatus == PGASYNC_COPY_BOTH) {
        if (pqPutMsgStart(PqMsg_CopyDone, conn) < 0 ||
            pqPutMsgEnd(conn) < 0)
            return 1;

        // Send Sync for extended-query mode
        if (conn->cmd_queue_head &&
            conn->cmd_queue_head->queryclass != PGQUERY_SIMPLE) {
            if (pqPutMsgStart(PqMsg_Sync, conn) < 0 ||
                pqPutMsgEnd(conn) < 0)
                return 1;
        }
    }

    // Flush outgoing data
    if (pqFlush(conn) && pqIsnonblocking(conn))
        return 1;

    // Return to busy state
    conn->asyncStatus = PGASYNC_BUSY;

    // Handle non-blocking connections
    if (pqIsnonblocking(conn) && PQisBusy(conn))
        return 1;

    // Wait for completion response
    PGresult *result = PQgetResult(conn);

    // Check for successful completion
    if (result && result->resultStatus == PGRES_COMMAND_OK) {
        PQclear(result);
        return 0;
    }

    // Handle errors by converting to notices (backwards compatibility)
    if (conn->errorMessage.len > 0) {
        // Strip trailing newline and send as notice
        char svLast = conn->errorMessage.data[conn->errorMessage.len - 1];
        if (svLast == '\n')
            conn->errorMessage.data[conn->errorMessage.len - 1] = '\0';
        pqInternalNotice(&conn->noticeHooks, "%s", conn->errorMessage.data);
        conn->errorMessage.data[conn->errorMessage.len - 1] = svLast;
    }

    PQclear(result);
    return 1;
}
```