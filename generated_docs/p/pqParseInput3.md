# pqParseInput3

## Location
[src/interfaces/libpq/fe-protocol3.c:66-482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L66-L482)

## Overview
pqParseInput3 is the main message parsing function for PostgreSQL's protocol version 3, responsible for processing all incoming messages from the backend server until input is exhausted or a stopping state is reached.

## Definition

```c
void
pqParseInput3(PGconn *conn)
```
## Detailed Description
This function implements the core message processing loop for the PostgreSQL client-server protocol version 3. It continuously reads and processes complete messages from the input buffer, handling various message types including query results, notifications, errors, and protocol control messages. The function validates message headers, manages connection state transitions, and dispatches messages to appropriate handlers based on the current connection state (IDLE, BUSY, COPY modes, etc.).

The function operates in a stateful manner, respecting the connection's async status and handling special cases like NOTIFY/NOTICE messages that can arrive at any time, versus other messages that should only be processed during specific states. It includes robust error handling for malformed messages and implements protocol validation to detect synchronization loss.

## Parameters / Member Variables
- `*conn`: Pointer to the PGconn structure representing the database connection, containing input buffer, connection state, and result information
## Dependencies
- Functions called/Symbols referenced:
  - [pqGetc](pqGetc.md) (reads single character from input buffer)
  - [pqGetInt](pqGetInt.md) (reads integer values from input buffer)
  - [handleSyncLoss](../h/handleSyncLoss.md) (handles protocol synchronization errors)
  - VALID_LONG_MESSAGE_TYPE (macro validating message types for large messages)
  - [pqCheckInBufferSpace](pqCheckInBufferSpace.md) (ensures sufficient buffer space)
  - [getNotify](../g/getNotify.md) (processes notification messages)
  - [pqGetErrorNotice3](pqGetErrorNotice3.md) (processes error and notice messages)
  - [getParameterStatus](../g/getParameterStatus.md) (processes parameter status messages)
  - [getReadyForQuery](../g/getReadyForQuery.md) (processes ready-for-query messages)
  - [getRowDescriptions](../g/getRowDescriptions.md) (processes row description messages)
  - [getParamDescriptions](../g/getParamDescriptions.md) (processes parameter description messages)
  - [getAnotherTuple](../g/getAnotherTuple.md) (processes data row messages)
  - [getCopyStart](../g/getCopyStart.md) (initiates COPY operations)
  - [pqTraceOutputMessage](pqTraceOutputMessage.md) (debug tracing)
  - [PQmakeEmptyPGresult](../P/PQmakeEmptyPGresult.md) (creates empty result objects)
  - [pqSaveErrorResult](pqSaveErrorResult.md) (saves error results)
  - [pqCommandQueueAdvance](pqCommandQueueAdvance.md) (advances command queue)
- Called from (representative examples):
  - [parseInput](parseInput.md) (from src/interfaces/libpq/fe-exec.c:2022)
  - pgunlock_thread (from src/interfaces/libpq/libpq-int.h:720)

## Notes and Other Information
- The function does NOT attempt to read more data from the backend; it only processes what's already in the input buffer
- Implements comprehensive message type handling for protocol version 3 including all standard PostgreSQL message types
- Maintains strict state machine behavior, ensuring messages are processed only in appropriate connection states
- Includes sophisticated error recovery mechanisms for malformed messages and buffer management
- Critical for libpq's asynchronous operation model, allowing non-blocking query processing
- The parsing loop continues until either the input buffer is exhausted or a state change requires stopping

## Simplified Source

```c
void pqParseInput3(PGconn *conn) {
    char id;
    int msgLength;
    int avail;

    // Main message processing loop
    for (;;) {
        // Read message type and length
        conn->inCursor = conn->inStart;
        if (pqGetc(&id, conn) || pqGetInt(&msgLength, 4, conn))
            return;

        // Validate message format
        if (msgLength < 4 || (msgLength > 30000 && !VALID_LONG_MESSAGE_TYPE(id))) {
            handleSyncLoss(conn, id, msgLength);
            return;
        }

        // Check if complete message is available
        msgLength -= 4;
        avail = conn->inEnd - conn->inCursor;
        if (avail < msgLength) {
            // Ensure buffer can hold the message
            if (pqCheckInBufferSpace(conn->inCursor + msgLength, conn))
                handleSyncLoss(conn, id, msgLength);
            return;
        }

        // Handle special messages that can arrive in any state
        if (id == PqMsg_NotificationResponse) {
            if (getNotify(conn)) return;
        }
        else if (id == PqMsg_NoticeResponse) {
            if (pqGetErrorNotice3(conn, false)) return;
        }
        // Handle messages based on connection state
        else if (conn->asyncStatus != PGASYNC_BUSY) {
            if (conn->asyncStatus != PGASYNC_IDLE) return;

            // Handle unexpected messages in IDLE state
            if (id == PqMsg_ErrorResponse) {
                if (pqGetErrorNotice3(conn, false)) return;
            }
            else if (id == PqMsg_ParameterStatus) {
                if (getParameterStatus(conn)) return;
            }
            else {
                // Skip unexpected message
                pqInternalNotice(&conn->noticeHooks,
                    "message type 0x%02x arrived from server while idle", id);
                conn->inCursor += msgLength;
            }
        }
        else {
            // Process messages in BUSY state
            switch (id) {
                case PqMsg_CommandComplete:
                    // Handle command completion
                    if (pqGets(&conn->workBuffer, conn)) return;
                    if (!pgHavePendingResult(conn)) {
                        conn->result = PQmakeEmptyPGresult(conn, PGRES_COMMAND_OK);
                        if (!conn->result) {
                            libpq_append_conn_error(conn, "out of memory");
                            pqSaveErrorResult(conn);
                        }
                    }
                    if (conn->result)
                        strlcpy(conn->result->cmdStatus, conn->workBuffer.data, CMDSTATUS_LEN);
                    conn->asyncStatus = PGASYNC_READY;
                    break;

                case PqMsg_ErrorResponse:
                    if (pqGetErrorNotice3(conn, true)) return;
                    conn->asyncStatus = PGASYNC_READY;
                    break;

                case PqMsg_ReadyForQuery:
                    if (getReadyForQuery(conn)) return;
                    // Handle pipeline or normal query completion
                    if (conn->pipelineStatus != PQ_PIPELINE_OFF) {
                        conn->result = PQmakeEmptyPGresult(conn, PGRES_PIPELINE_SYNC);
                        if (!conn->result) {
                            libpq_append_conn_error(conn, "out of memory");
                            pqSaveErrorResult(conn);
                        } else {
                            conn->pipelineStatus = PQ_PIPELINE_ON;
                            conn->asyncStatus = PGASYNC_READY;
                        }
                    } else {
                        pqCommandQueueAdvance(conn, true, false);
                        conn->asyncStatus = PGASYNC_IDLE;
                    }
                    break;

                case PqMsg_RowDescription:
                    // Handle row description or skip if in error state
                    if (conn->error_result ||
                        (conn->result && conn->result->resultStatus == PGRES_FATAL_ERROR)) {
                        conn->inCursor += msgLength;
                    }
                    else if (!conn->result ||
                             (conn->cmd_queue_head &&
                              conn->cmd_queue_head->queryclass == PGQUERY_DESCRIBE)) {
                        if (getRowDescriptions(conn, msgLength)) return;
                    }
                    else {
                        conn->asyncStatus = PGASYNC_READY;
                        return;
                    }
                    break;

                case PqMsg_DataRow:
                    // Process data rows for query results
                    if (conn->result &&
                        (conn->result->resultStatus == PGRES_TUPLES_OK ||
                         conn->result->resultStatus == PGRES_TUPLES_CHUNK)) {
                        if (getAnotherTuple(conn, msgLength)) return;
                    }
                    else if (conn->error_result ||
                             (conn->result && conn->result->resultStatus == PGRES_FATAL_ERROR)) {
                        conn->inCursor += msgLength;
                    }
                    else {
                        libpq_append_conn_error(conn, "server sent data without row description");
                        pqSaveErrorResult(conn);
                        conn->inCursor += msgLength;
                    }
                    break;

                // Handle various COPY operations
                case PqMsg_CopyInResponse:
                    if (getCopyStart(conn, PGRES_COPY_IN)) return;
                    conn->asyncStatus = PGASYNC_COPY_IN;
                    break;

                case PqMsg_CopyOutResponse:
                    if (getCopyStart(conn, PGRES_COPY_OUT)) return;
                    conn->asyncStatus = PGASYNC_COPY_OUT;
                    conn->copy_already_done = 0;
                    break;

                default:
                    // Handle other message types (ParseComplete, BindComplete, etc.)
                    // or report unexpected messages
                    libpq_append_conn_error(conn, "unexpected response from server");
                    pqSaveErrorResult(conn);
                    conn->asyncStatus = PGASYNC_READY;
                    conn->inCursor += msgLength;
                    break;
            }
        }

        // Validate message consumption and advance buffer position
        if (conn->inCursor == conn->inStart + 5 + msgLength) {
            if (conn->Pfdebug)
                pqTraceOutputMessage(conn, conn->inBuffer + conn->inStart, false);
            conn->inStart = conn->inCursor;
        }
        else {
            libpq_append_conn_error(conn, "message contents do not agree with length");
            pqSaveErrorResult(conn);
            conn->asyncStatus = PGASYNC_READY;
            conn->inStart += 5 + msgLength;
        }
    }
}
```