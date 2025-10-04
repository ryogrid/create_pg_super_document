# pqFunctionCall3

## Location
[src/interfaces/libpq/fe-protocol3.c:2009-2236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L2009-L2236)

## Overview
Executes PostgreSQL server-side function calls using protocol 3, handling the complete message exchange including parameter serialization, response processing, and error handling.

## Definition

```c
PGresult *
pqFunctionCall3(PGconn *conn, Oid fnid,
				int *result_buf, int *actual_result_len,
				int result_is_int,
				const PQArgBlock *args, int nargs)
```
## Detailed Description
pqFunctionCall3 implements the protocol 3 function call mechanism for PostgreSQL, enabling clients to execute server-side functions directly. It constructs and sends a FunctionCall message with the function OID and serialized arguments, then processes the server response. The function handles both integer and binary data arguments, manages message framing, validates message integrity, and processes various response types including function results, errors, notices, and notifications.

The function uses a state machine approach to process incoming messages, handling partial reads and ensuring proper synchronization with the server protocol.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle for the database session
- `fnid`: Object ID (OID) of the server-side function to execute
- `*result_buf`: Buffer to store the function's return value
- `*actual_result_len`: Pointer to store the actual length of the returned result
- `result_is_int`: Flag indicating whether the result should be treated as an integer
- `*args`: Array of PQArgBlock structures containing function arguments
- `nargs`: Number of arguments in the args array
## Dependencies
- Functions called/Symbols referenced:
  - [pqPutMsgStart](pqPutMsgStart.md), pqPutMsgEnd
  - [pqPutInt](pqPutInt.md), pqPutnchar
  - [pqFlush](pqFlush.md), pqWait, pqReadData
  - [pqGetc](pqGetc.md), pqGetInt, pqGetnchar
  - [handleSyncLoss](../h/handleSyncLoss.md), pqCheckInBufferSpace
  - [pqGetErrorNotice3](pqGetErrorNotice3.md), getNotify, getReadyForQuery, getParameterStatus
  - pgHavePendingResult, PQmakeEmptyPGresult
  - [pqSaveErrorResult](pqSaveErrorResult.md), pqPrepareAsyncResult
  - [pqTraceOutputMessage](pqTraceOutputMessage.md)
  - PqMsg_FunctionCall (message type)
  - PGRES_COMMAND_OK, PGRES_FATAL_ERROR (result status constants)
  - PQ_PIPELINE_OFF (pipeline status)
  - VALID_LONG_MESSAGE_TYPE (message validation macro)
- Called from (representative examples):
  - [PQfn](../P/PQfn.md) (in src/interfaces/libpq/fe-exec.c)

## Notes and Other Information
- Returns a PGresult pointer containing the function result or error information
- Supports both integer and binary argument types through the PQArgBlock structure
- Handles NULL arguments by setting len to -1 in the argument structure
- Uses binary format for both input arguments and output results
- Implements comprehensive message validation including length checks and type validation
- Processes various message types: 'V' (function result), 'E' (error), 'A' (notify), 'N' (notice), 'Z' (ready for query), 'S' (parameter status)
- Maintains protocol synchronization and handles partial message reads
- Part of the libpq protocol 3 implementation for PostgreSQL client-server communication
- Includes debug tracing support when conn->Pfdebug is enabled

## Simplified Source

```c
PGresult *pqFunctionCall3(PGconn *conn, Oid fnid,
                         int *result_buf, int *actual_result_len,
                         int result_is_int,
                         const PQArgBlock *args, int nargs) {
    bool needInput = false;
    ExecStatusType status = PGRES_FATAL_ERROR;
    char id;
    int msgLength;
    int avail;
    int i;

    // Build and send FunctionCall message
    if (pqPutMsgStart(PqMsg_FunctionCall, conn) < 0 ||
        pqPutInt(fnid, 4, conn) < 0 ||        // Function OID
        pqPutInt(1, 2, conn) < 0 ||           // Format codes count
        pqPutInt(1, 2, conn) < 0 ||           // Binary format
        pqPutInt(nargs, 2, conn) < 0)         // Argument count
        return NULL;

    // Send each argument
    for (i = 0; i < nargs; ++i) {
        if (pqPutInt(args[i].len, 4, conn))
            return NULL;

        if (args[i].len == -1)
            continue;  // NULL argument

        // Send argument data (integer or binary)
        if (args[i].isint) {
            if (pqPutInt(args[i].u.integer, args[i].len, conn))
                return NULL;
        } else {
            if (pqPutnchar((char *) args[i].u.ptr, args[i].len, conn))
                return NULL;
        }
    }

    // Send result format (binary) and flush
    if (pqPutInt(1, 2, conn) < 0 || pqPutMsgEnd(conn) < 0 || pqFlush(conn))
        return NULL;

    // Process server responses
    for (;;) {
        if (needInput) {
            if (pqWait(true, false, conn) || pqReadData(conn) < 0)
                break;
        }

        needInput = true;
        conn->inCursor = conn->inStart;

        // Read message header
        if (pqGetc(&id, conn) || pqGetInt(&msgLength, 4, conn))
            continue;

        // Validate message
        if (msgLength < 4) {
            handleSyncLoss(conn, id, msgLength);
            break;
        }

        msgLength -= 4;
        avail = conn->inEnd - conn->inCursor;
        if (avail < msgLength) {
            if (pqCheckInBufferSpace(conn->inCursor + msgLength, conn)) {
                handleSyncLoss(conn, id, msgLength);
                break;
            }
            continue;
        }

        // Process message by type
        switch (id) {
            case 'V':  // Function result
                if (pqGetInt(actual_result_len, 4, conn))
                    continue;
                if (*actual_result_len != -1) {
                    if (result_is_int) {
                        if (pqGetInt(result_buf, *actual_result_len, conn))
                            continue;
                    } else {
                        if (pqGetnchar((char *) result_buf, *actual_result_len, conn))
                            continue;
                    }
                }
                status = PGRES_COMMAND_OK;
                break;

            case 'E':  // Error
                if (pqGetErrorNotice3(conn, true))
                    continue;
                status = PGRES_FATAL_ERROR;
                break;

            case 'Z':  // Ready for query (end of transaction)
                if (getReadyForQuery(conn))
                    continue;
                conn->inStart += 5 + msgLength;

                // Create result object
                if (!pgHavePendingResult(conn)) {
                    if (status == PGRES_COMMAND_OK) {
                        conn->result = PQmakeEmptyPGresult(conn, status);
                        if (!conn->result) {
                            libpq_append_conn_error(conn, "out of memory");
                            pqSaveErrorResult(conn);
                        }
                    } else {
                        libpq_append_conn_error(conn, "protocol error: no function result");
                        pqSaveErrorResult(conn);
                    }
                }
                return pqPrepareAsyncResult(conn);

            case 'A':  // Notify
                if (getNotify(conn))
                    continue;
                break;

            case 'N':  // Notice
                if (pqGetErrorNotice3(conn, false))
                    continue;
                break;

            case 'S':  // Parameter status
                if (getParameterStatus(conn))
                    continue;
                break;

            default:
                libpq_append_conn_error(conn, "protocol error: id=0x%x", id);
                pqSaveErrorResult(conn);
                conn->inStart += 5 + msgLength;
                return pqPrepareAsyncResult(conn);
        }

        // Mark message as processed
        conn->inStart += 5 + msgLength;
        needInput = false;
    }

    // Network error occurred
    pqSaveErrorResult(conn);
    return pqPrepareAsyncResult(conn);
}
```