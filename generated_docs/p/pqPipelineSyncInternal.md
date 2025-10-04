# pqPipelineSyncInternal

## Location
[src/interfaces/libpq/fe-exec.c:3294-3370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3294-L3370)

## Overview
pqPipelineSyncInternal is a static helper function that implements the core logic for sending pipeline synchronization messages in PostgreSQL's libpq client library.

## Definition

```c
struct the Sync message */
	if (pqPutMsgStart(PqMsg_Sync, conn) < 0 ||
		pqPutMsgEnd(conn) < 0)
		goto sendFailed;
```
## Detailed Description
This function serves as the workhorse implementation for both PQpipelineSync and PQsendPipelineSync functions. It constructs and sends a Sync message to the PostgreSQL server to mark a synchronization point in a pipeline of queries. The function validates the connection state, ensures pipeline mode is active, constructs the appropriate protocol message, and handles the sending process with optional immediate flushing.

The function performs several key operations:
- Validates that the connection is in pipeline mode (not PQ_PIPELINE_OFF)
- Checks that the connection is in a valid state for sending sync messages
- Allocates a command queue entry for tracking the sync operation
- Constructs a Sync protocol message using the PostgreSQL wire protocol
- Handles message transmission with configurable flushing behavior
- Updates the connection's command queue to track the pending sync

## Parameters / Member Variables
- : PostgreSQL connection handle that must be in pipeline mode
- : Boolean flag controlling whether to flush the message immediately (true) or use threshold-based flushing (false)

## Dependencies
- Functions called/Symbols referenced:
  - [pqAllocCmdQueueEntry](pqAllocCmdQueueEntry.md)
  - [pqPutMsgStart](pqPutMsgStart.md)
  - [pqPutMsgEnd](pqPutMsgEnd.md)
  - [pqFlush](pqFlush.md)
  - [pqPipelineFlush](pqPipelineFlush.md)
  - [pqAppendCmdQueueEntry](pqAppendCmdQueueEntry.md)
  - [pqRecycleCmdQueueEntry](pqRecycleCmdQueueEntry.md)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
- Called from:
  - [PQpipelineSync](../P/PQpipelineSync.md) (fe-exec.c:3274)
  - [PQsendPipelineSync](../P/PQsendPipelineSync.md) (fe-exec.c:3284)

## Notes and Other Information
- This is an internal static function not exposed in the public libpq API
- The function validates connection async status, rejecting sync operations during COPY operations
- Error handling includes proper cleanup of allocated command queue entries on failure
- The immediate_flush parameter allows callers to control when data is actually sent to the server
- Returns 1 on success, 0 on failure with error message set in the connection object
- The function is located at src/interfaces/libpq/fe-exec.c:3294-3370

## Simplified Source

```c
static int pqPipelineSyncInternal(PGconn *conn, bool immediate_flush)
{
    PGcmdQueueEntry *entry;

    if (!conn)
        return 0;

    // Verify connection is in pipeline mode
    if (conn->pipelineStatus == PQ_PIPELINE_OFF)
    {
        libpq_append_conn_error(conn, "cannot send pipeline when not in pipeline mode");
        return 0;
    }

    // Validate connection state - reject COPY operations
    switch (conn->asyncStatus)
    {
        case PGASYNC_COPY_IN:
        case PGASYNC_COPY_OUT:
        case PGASYNC_COPY_BOTH:
            appendPQExpBufferStr(&conn->errorMessage, "internal error: cannot send pipeline while in COPY\n");
            return 0;
        case PGASYNC_READY:
        case PGASYNC_READY_MORE:
        case PGASYNC_BUSY:
        case PGASYNC_IDLE:
        case PGASYNC_PIPELINE_IDLE:
            // OK to proceed
            break;
    }

    // Allocate command queue entry for sync operation
    entry = pqAllocCmdQueueEntry(conn);
    if (entry == NULL)
        return 0;

    entry->queryclass = PGQUERY_SYNC;
    entry->query = NULL;

    // Construct and send Sync message
    if (pqPutMsgStart(PqMsg_Sync, conn) < 0 ||
        pqPutMsgEnd(conn) < 0)
        goto sendFailed;

    // Flush data according to immediate_flush setting
    if (immediate_flush)
    {
        if (pqFlush(conn) < 0)
            goto sendFailed;
    }
    else
    {
        if (pqPipelineFlush(conn) < 0)
            goto sendFailed;
    }

    // Success: add to command queue
    pqAppendCmdQueueEntry(conn, entry);
    return 1;

sendFailed:
    pqRecycleCmdQueueEntry(conn, entry);
    return 0;
}
```