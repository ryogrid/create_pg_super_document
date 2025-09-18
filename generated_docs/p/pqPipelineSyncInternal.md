# pqPipelineSyncInternal

## Location
src/interfaces/libpq/fe-exec.c: 3294 - 3370

## Overview
pqPipelineSyncInternal is a static helper function that implements the core logic for sending pipeline synchronization messages in PostgreSQL's libpq client library.

## Definition


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
  - PQpipelineSync (fe-exec.c:3274)
  - PQsendPipelineSync (fe-exec.c:3284)

## Notes and Other Information
- This is an internal static function not exposed in the public libpq API
- The function validates connection async status, rejecting sync operations during COPY operations
- Error handling includes proper cleanup of allocated command queue entries on failure
- The immediate_flush parameter allows callers to control when data is actually sent to the server
- Returns 1 on success, 0 on failure with error message set in the connection object
- The function is located at src/interfaces/libpq/fe-exec.c:3294-3370