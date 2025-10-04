# PQsendTypedCommand

## Location
[src/interfaces/libpq/fe-exec.c:2589-2666](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2589-L2666)

## Overview
PQsendTypedCommand is a common internal utility function that constructs and sends Describe or Close commands to the PostgreSQL server, handling the low-level message protocol details.

## Definition
```c
static int PQsendTypedCommand(PGconn *conn, char command, char type, const char *target)
```

## Detailed Description
PQsendTypedCommand is a static internal function that provides the common implementation for sending typed commands (Describe or Close) to the PostgreSQL server. It handles the construction of the appropriate protocol messages, manages command queue entries, and supports both regular and pipeline execution modes.

The function constructs protocol messages according to the PostgreSQL wire protocol, including the main command message and a Sync message (when not in pipeline mode). It properly manages the command queue by allocating entries and tracking query types for result processing. The function also handles pipeline flushing and error recovery through proper cleanup of allocated resources.

## Parameters / Member Variables
- `conn`: Connection handle to the PostgreSQL database server
- `command`: Command type - either PqMsg_Close for Close commands or PqMsg_Describe for Describe commands
- `type`: Target type - 'S' for prepared statements or 'P' for portals
- `target`: Name of the target object (prepared statement or portal name), treated as empty string if NULL

## Dependencies
- Functions called/Symbols referenced:
  - [PQsendQueryStart](PQsendQueryStart.md)
  - [pqAllocCmdQueueEntry](../p/pqAllocCmdQueueEntry.md)
  - [pqPutMsgStart](../p/pqPutMsgStart.md)
  - [pqPutc](../p/pqPutc.md)
  - [pqPuts](../p/pqPuts.md)
  - [pqPutMsgEnd](../p/pqPutMsgEnd.md)
  - [pqPipelineFlush](../p/pqPipelineFlush.md)
  - [pqAppendCmdQueueEntry](../p/pqAppendCmdQueueEntry.md)
  - [pqRecycleCmdQueueEntry](../p/pqRecycleCmdQueueEntry.md)
- Called from (representative examples):
  - [PQdescribePrepared](PQdescribePrepared.md)
  - [PQdescribePortal](PQdescribePortal.md)
  - [PQsendDescribePrepared](PQsendDescribePrepared.md)
  - [PQsendDescribePortal](PQsendDescribePortal.md)
  - [PQclosePrepared](PQclosePrepared.md)
  - [PQclosePortal](PQclosePortal.md)
  - [PQsendClosePrepared](PQsendClosePrepared.md)
  - [PQsendClosePortal](PQsendClosePortal.md)

## Notes and Other Information
- Returns 1 on success, 0 on failure (with error message set in conn->errorMessage)
- This is a static function, not part of the public libpq API
- Handles both pipeline and non-pipeline execution modes appropriately
- Automatically adds Sync message when not in pipeline mode to ensure command completion
- Supports proper error recovery by recycling allocated command queue entries on failure
- Central implementation point for all Describe and Close command variants in libpq

## Simplified Source

```c
static int
PQsendTypedCommand(PGconn *conn, char command, char type, const char *target)
{
    PGcmdQueueEntry *entry = NULL;

    // Use empty string for null target
    if (!target)
        target = "";

    // Prepare connection for sending query
    if (!PQsendQueryStart(conn, true))
        return 0;

    // Allocate command queue entry
    entry = pqAllocCmdQueueEntry(conn);
    if (entry == NULL)
        return 0;

    // Build the command message (Close/Describe + type + target)
    if (pqPutMsgStart(command, conn) < 0 ||
        pqPutc(type, conn) < 0 ||
        pqPuts(target, conn) < 0 ||
        pqPutMsgEnd(conn) < 0)
        goto sendFailed;

    // Add Sync message when not in pipeline mode
    if (conn->pipelineStatus == PQ_PIPELINE_OFF) {
        if (pqPutMsgStart(PqMsg_Sync, conn) < 0 ||
            pqPutMsgEnd(conn) < 0)
            goto sendFailed;
    }

    // Set query class based on command type
    if (command == PqMsg_Close)
        entry->queryclass = PGQUERY_CLOSE;
    else if (command == PqMsg_Describe)
        entry->queryclass = PGQUERY_DESCRIBE;
    else {
        libpq_append_conn_error(conn, "unrecognized message type \"%c\"", command);
        goto sendFailed;
    }

    // Flush data and add to command queue
    if (pqPipelineFlush(conn) < 0)
        goto sendFailed;

    pqAppendCmdQueueEntry(conn, entry);
    return 1;

sendFailed:
    pqRecycleCmdQueueEntry(conn, entry);
    return 0;
}
```