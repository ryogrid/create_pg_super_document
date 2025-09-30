# pqPipelineProcessQueue

## Location
[src/interfaces/libpq/fe-exec.c:3180-3271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3180-L3271)

## Overview
Processes the next query in the pipeline queue, managing state transitions and error handling for pipelined command execution.

## Definition

```c
static void
pqPipelineProcessQueue(PGconn *conn)
```
## Detailed Description
pqPipelineProcessQueue is a subroutine for PQgetResult that manages the processing of queued commands in pipeline mode. It handles state transitions between different async states, processes the next command in the queue when appropriate, and manages special cases like aborted pipelines.

The function first checks if the connection is in a state where it can process the next query (not busy with current operations). It then transitions the connection to the appropriate state, resets result accumulation modes, and either prepares for normal query processing or handles aborted pipeline scenarios by generating PGRES_PIPELINE_ABORTED results.

## Parameters / Member Variables
- : The PostgreSQL connection containing the command queue to process

## Dependencies
- Functions called/Symbols referenced:
  - pqClearConnErrorState
  - [pqClearAsyncResult](pqClearAsyncResult.md)
  - [PQmakeEmptyPGresult](../P/PQmakeEmptyPGresult.md)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - [pqSaveErrorResult](pqSaveErrorResult.md)
  - PGASYNC_COPY_IN, PGASYNC_COPY_OUT, PGASYNC_COPY_BOTH
  - PGASYNC_READY, PGASYNC_READY_MORE, PGASYNC_BUSY
  - PGASYNC_IDLE, PGASYNC_PIPELINE_IDLE
  - PQ_PIPELINE_OFF, PQ_PIPELINE_ABORTED
  - PGQUERY_SYNC
  - PGRES_PIPELINE_ABORTED

- Called from (representative examples):
  - [pqAppendCmdQueueEntry](pqAppendCmdQueueEntry.md) (fe-exec.c)
  - [PQgetResult](../P/PQgetResult.md) (fe-exec.c)

## Notes and Other Information
- Only processes when connection is idle or in pipeline-idle state
- Resets partial result modes (partialResMode, singleRowMode, maxChunkSize) for each new query
- Transitions to PGASYNC_IDLE when command queue is empty
- Generates PGRES_PIPELINE_ABORTED results for non-SYNC commands in aborted pipelines
- Prepares connection state for next query processing by clearing error state and async results
- Critical component of PostgreSQL's pipeline mode implementation

## Simplified Source

```c
static void
pqPipelineProcessQueue(PGconn *conn)
{
    // Check if we can process next query
    switch (conn->asyncStatus) {
        case PGASYNC_COPY_IN:
        case PGASYNC_COPY_OUT:
        case PGASYNC_COPY_BOTH:
        case PGASYNC_READY:
        case PGASYNC_READY_MORE:
        case PGASYNC_BUSY:
            // Client still processing current query
            return;

        case PGASYNC_IDLE:
            // Transition to pipeline mode if commands are queued
            if (conn->cmd_queue_head != NULL) {
                conn->asyncStatus = PGASYNC_PIPELINE_IDLE;
                break;
            }
            return;

        case PGASYNC_PIPELINE_IDLE:
            // Ready to process next query
            break;
    }

    // Reset partial result modes for new query
    conn->partialResMode = false;
    conn->singleRowMode = false;
    conn->maxChunkSize = 0;

    // Return to idle if no more commands
    if (conn->cmd_queue_head == NULL) {
        conn->asyncStatus = PGASYNC_IDLE;
        return;
    }

    // Prepare for next query
    pqClearConnErrorState(conn);
    pqClearAsyncResult(conn);

    // Handle aborted pipeline
    if (conn->pipelineStatus == PQ_PIPELINE_ABORTED &&
        conn->cmd_queue_head->queryclass != PGQUERY_SYNC) {
        // Generate aborted result for non-SYNC commands
        conn->result = PQmakeEmptyPGresult(conn, PGRES_PIPELINE_ABORTED);
        if (!conn->result) {
            libpq_append_conn_error(conn, "out of memory");
            pqSaveErrorResult(conn);
            return;
        }
        conn->asyncStatus = PGASYNC_READY;
    } else {
        // Allow normal parsing to continue
        conn->asyncStatus = PGASYNC_BUSY;
    }
}
```