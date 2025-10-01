# pqGetErrorNotice3

## Location
[src/interfaces/libpq/fe-protocol3.c:882-1013](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L882-L1013)

## Overview
Processes Error or Notice response messages from the PostgreSQL server in protocol version 3, handling both error conditions and informational notices.

## Definition

```c
int
pqGetErrorNotice3(PGconn *conn, bool isError)
```
## Detailed Description
This function reads and processes Error ('E') or Notice ('N') messages from the PostgreSQL server using protocol version 3. The message type and length have already been consumed before this function is called. It creates a PGresult structure to hold the error/notice fields, builds a formatted error message, and either stores it as an async result (for errors) or processes it as a notice callback.

The function handles pipeline mode by setting the pipeline status to aborted when an error occurs. For errors, it pre-emptively clears any incomplete query results to avoid memory issues. It reads all message fields in a loop until a null terminator is found, saving special fields like SQLSTATE and statement position for later use.

## Parameters / Member Variables
- : PostgreSQL connection object containing connection state and buffers
- : Boolean flag indicating whether this is an Error (true) or Notice (false) message

## Dependencies
- Functions called/Symbols referenced:
  - [pqClearAsyncResult](pqClearAsyncResult.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [PQmakeEmptyPGresult](../P/PQmakeEmptyPGresult.md)
  - [pqGetc](pqGetc.md)
  - [pqGets](pqGets.md)
  - [pqSaveMessageField](pqSaveMessageField.md)
  - [strlcpy](../s/strlcpy.md)
  - [pqResultStrdup](pqResultStrdup.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - [pqBuildErrorMessage3](pqBuildErrorMessage3.md)
  - [pqSetResultError](pqSetResultError.md)
  - PQExpBufferDataBroken
  - [libpq_gettext](../l/libpq_gettext.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
- Called from (representative examples):
  - [pqParseInput3](pqParseInput3.md)
  - [getCopyDataMessage](../g/getCopyDataMessage.md)
  - [pqFunctionCall3](pqFunctionCall3.md)

## Notes and Other Information
- Returns 0 on successful message consumption, EOF if insufficient data available
- Uses a temporary PQExpBuffer instead of conn->workBuffer for potentially long field data
- Saves SQLSTATE in conn->last_sqlstate for error tracking
- Handles memory allocation failures gracefully by falling back to internal error processing
- For errors in pipeline mode, sets pipeline status to PQ_PIPELINE_ABORTED
- Notice messages trigger registered notice callbacks if available
- [Query](../Q/Query.md) text is preserved in the result only when statement position information is present

## Simplified Source

```c
int pqGetErrorNotice3(PGconn *conn, bool isError)
{
    PGresult *res = NULL;
    bool have_position = false;
    PQExpBufferData workBuf;
    char id;

    // Set pipeline error status if needed
    if (isError && conn->pipelineStatus != PQ_PIPELINE_OFF)
        conn->pipelineStatus = PQ_PIPELINE_ABORTED;

    // Clear any incomplete query result for errors
    if (isError)
        pqClearAsyncResult(conn);

    // Initialize temporary buffer for potentially long field data
    initPQExpBuffer(&workBuf);

    // Create result structure to hold error/notice fields
    res = PQmakeEmptyPGresult(conn, PGRES_EMPTY_QUERY);
    if (res)
        res->resultStatus = isError ? PGRES_FATAL_ERROR : PGRES_NONFATAL_ERROR;

    // Read all message fields until null terminator
    for (;;) {
        if (pqGetc(&id, conn))
            goto fail;
        if (id == '\0')
            break;  // terminator found

        if (pqGets(&workBuf, conn))
            goto fail;

        pqSaveMessageField(res, id, workBuf.data);

        // Save special fields
        if (id == PG_DIAG_SQLSTATE)
            strlcpy(conn->last_sqlstate, workBuf.data, sizeof(conn->last_sqlstate));
        else if (id == PG_DIAG_STATEMENT_POSITION)
            have_position = true;
    }

    // Save query text if position info is available
    if (have_position && res && conn->cmd_queue_head && conn->cmd_queue_head->query)
        res->errQuery = pqResultStrdup(res, conn->cmd_queue_head->query);

    // Build formatted error message
    resetPQExpBuffer(&workBuf);
    pqBuildErrorMessage3(&workBuf, res, conn->verbosity, conn->show_context);

    // Handle result based on message type
    if (isError) {
        // Store as async result for errors
        pqClearAsyncResult(conn);
        if (res) {
            pqSetResultError(res, &workBuf, 0);
            conn->result = res;
        } else {
            conn->error_result = true;
        }

        // Append to connection error message
        if (PQExpBufferDataBroken(workBuf))
            libpq_append_conn_error(conn, "out of memory");
        else
            appendPQExpBufferStr(&conn->errorMessage, workBuf.data);
    } else {
        // Process as notice
        if (res) {
            if (PQExpBufferDataBroken(workBuf))
                res->errMsg = libpq_gettext("out of memory\n");
            else
                res->errMsg = workBuf.data;

            // Trigger notice callback if registered
            if (res->noticeHooks.noticeRec != NULL)
                res->noticeHooks.noticeRec(res->noticeHooks.noticeRecArg, res);
            PQclear(res);
        }
    }

    termPQExpBuffer(&workBuf);
    return 0;

fail:
    PQclear(res);
    termPQExpBuffer(&workBuf);
    return EOF;
}
```