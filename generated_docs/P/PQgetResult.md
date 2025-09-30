# PQgetResult

## Location
[src/interfaces/libpq/fe-exec.c:2062-2223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2062-L2223)

## Overview
Core libpq function that retrieves the next PGresult from a query, handling both synchronous blocking and asynchronous non-blocking operation modes including pipeline processing.

## Definition

```c
PGresult *
PQgetResult(PGconn *conn)
```
## Detailed Description
PQgetResult is a fundamental function in libpq that retrieves query results from the server. It operates in multiple modes depending on the connection state and pipeline configuration. The function first attempts to parse any available buffered data, then enters a blocking loop if necessary to wait for complete results.

In normal operation, the function handles various asynchronous states including BUSY (query in progress), READY (result available), and IDLE (query complete). For pipeline mode, it supports PIPELINE_IDLE state and PGRES_PIPELINE_SYNC results to manage batched queries efficiently.

The function performs I/O operations when needed, including flushing outbound data with pqFlush, waiting for socket readiness with pqWait, and reading incoming data with pqReadData. It handles error conditions gracefully by saving error states and transitioning to appropriate recovery states.

For COPY operations, the function delegates to getCopyResult to handle COPY_IN, COPY_OUT, and COPY_BOTH states. The function also supports chunked tuple processing (PGRES_TUPLES_CHUNK) for large result sets.

## Parameters / Member Variables
- : Pointer to the PGconn structure representing the database connection from which to retrieve results

## Dependencies
- Functions called/Symbols referenced:
  - [parseInput](../p/parseInput.md)
  - [pqFlush](../p/pqFlush.md)
  - [pqWait](../p/pqWait.md)
  - [pqReadData](../p/pqReadData.md)
  - [pqSaveErrorResult](../p/pqSaveErrorResult.md)
  - [pqPrepareAsyncResult](../p/pqPrepareAsyncResult.md)
  - [pqSaveWriteError](../p/pqSaveWriteError.md)
  - [pqPipelineProcessQueue](../p/pqPipelineProcessQueue.md)
  - [pqCommandQueueAdvance](../p/pqCommandQueueAdvance.md)
  - [getCopyResult](../g/getCopyResult.md)
  - [PQfireResultCreateEvents](PQfireResultCreateEvents.md)
- Called from (representative examples):
  - [PQexecStart](PQexecStart.md)
  - [PQexecFinish](PQexecFinish.md)
  - [libpqrcv_PQgetResult](../l/libpqrcv_PQgetResult.md)
  - [consumeQueryResult](../c/consumeQueryResult.md)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md)

## Notes and Other Information
- Returns NULL when no more results are available or connection is invalid
- Blocks until a result is available unless the connection is in a non-blocking state
- In pipeline mode, returns NULL between queries and PGRES_PIPELINE_SYNC at pipeline end
- Handles both single query and pipeline query processing
- Manages complex state transitions between different asynchronous states
- Part of the core public libpq API for result retrieval
- Fires PGEVT_RESULTCREATE events for event-driven applications
- Essential for both synchronous and asynchronous query processing patterns

## Simplified Source

```c
PGresult *PQgetResult(PGconn *conn) {
    if (!conn)
        return NULL;

    // Parse any available buffered data
    parseInput(conn);

    // Wait for result if busy
    while (conn->asyncStatus == PGASYNC_BUSY) {
        // Flush any pending outbound data
        while (pqFlush(conn) > 0) {
            if (pqWait(false, true, conn))
                break;
        }

        // Wait for incoming data and read it
        if (pqWait(true, false, conn) || pqReadData(conn) < 0) {
            pqSaveErrorResult(conn);
            conn->asyncStatus = PGASYNC_IDLE;
            return pqPrepareAsyncResult(conn);
        }

        // Parse the new data
        parseInput(conn);

        // Handle write errors
        if (conn->write_failed && conn->asyncStatus == PGASYNC_BUSY) {
            pqSaveWriteError(conn);
            conn->asyncStatus = PGASYNC_IDLE;
            return pqPrepareAsyncResult(conn);
        }
    }

    // Return result based on current state
    PGresult *res;
    switch (conn->asyncStatus) {
        case PGASYNC_IDLE:
            res = NULL;  // Query complete
            break;

        case PGASYNC_PIPELINE_IDLE:
            pqPipelineProcessQueue(conn);
            res = NULL;  // Query complete
            break;

        case PGASYNC_READY:
            res = pqPrepareAsyncResult(conn);

            // Handle chunked results
            if (conn->result && res->resultStatus == PGRES_TUPLES_CHUNK)
                break;

            // Advance command queue
            pqCommandQueueAdvance(conn, false,
                                res->resultStatus == PGRES_PIPELINE_SYNC);

            // Handle pipeline vs normal mode
            if (conn->pipelineStatus != PQ_PIPELINE_OFF) {
                conn->asyncStatus = PGASYNC_PIPELINE_IDLE;
                if (res->resultStatus == PGRES_PIPELINE_SYNC)
                    pqPipelineProcessQueue(conn);
            } else {
                conn->asyncStatus = PGASYNC_BUSY;
            }
            break;

        case PGASYNC_READY_MORE:
            res = pqPrepareAsyncResult(conn);
            conn->asyncStatus = PGASYNC_BUSY;
            break;

        case PGASYNC_COPY_IN:
            res = getCopyResult(conn, PGRES_COPY_IN);
            break;

        case PGASYNC_COPY_OUT:
            res = getCopyResult(conn, PGRES_COPY_OUT);
            break;

        case PGASYNC_COPY_BOTH:
            res = getCopyResult(conn, PGRES_COPY_BOTH);
            break;

        default:
            // Handle unexpected state
            libpq_append_conn_error(conn, "unexpected asyncStatus: %d",
                                   (int) conn->asyncStatus);
            pqSaveErrorResult(conn);
            conn->asyncStatus = PGASYNC_IDLE;
            res = pqPrepareAsyncResult(conn);
            break;
    }

    // Fire result creation events if needed
    if (res && res->nEvents > 0)
        PQfireResultCreateEvents(conn, res);

    return res;
}
```