# PQsendQueryStart

## Location
[src/interfaces/libpq/fe-exec.c:1673-1756](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L1673-L1756)

## Overview
PQsendQueryStart is a static function that provides common startup validation and state preparation for all PostgreSQL query sending functions.

## Definition

```c
static bool
PQsendQueryStart(PGconn *conn, bool newQuery)
```
## Detailed Description
PQsendQueryStart serves as the foundation for all query sending operations in libpq, performing essential connection state validation and preparation before any query can be sent to the PostgreSQL server. This function implements the common logic shared across PQsendQuery, PQsendQueryParams, PQsendPrepare, and PQsendQueryPrepared.

The function handles different operational modes including normal query execution and pipeline mode operations. It validates that the connection is in an appropriate state for sending queries, manages error state clearing for new query cycles, and handles the complex state machine requirements for pipeline mode operations. The function ensures that queries can only be sent when the connection is ready and not conflicting with ongoing operations like COPY commands.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle to validate and prepare for query sending
- `newQuery`: Boolean flag indicating whether this represents the start of a new query cycle (affects error state management)
## Dependencies
- Functions called/Symbols referenced:
  - pqClearConnErrorState: Clears the connection's error state buffer
  - [pqClearAsyncResult](../p/pqClearAsyncResult.md): Initializes asynchronous result accumulation state
  - CONNECTION_OK: Connection status constant indicating a healthy connection
  - Various PGASYNC_* constants: Asynchronous status values for connection state validation
  - PQ_PIPELINE_OFF: Pipeline status constant indicating normal (non-pipeline) mode
- Called from (representative examples):
  - [PQsendQueryInternal](PQsendQueryInternal.md): Simple query protocol implementation
  - [PQsendQueryParams](PQsendQueryParams.md): Parameterized query sending function
  - [PQsendPrepare](PQsendPrepare.md): Statement preparation function
  - [PQsendQueryPrepared](PQsendQueryPrepared.md): Prepared statement execution function
  - [PQsendTypedCommand](PQsendTypedCommand.md): Typed command sending function

## Notes and Other Information
- Serves as the central validation point for all query sending operations in libpq
- Implements complex state machine logic for pipeline mode operations, ensuring commands can be safely queued
- Manages error state clearing strategically - only clears errors for new query cycles when no commands are queued
- Prevents conflicting operations by checking connection and asynchronous status before allowing query sending
- Handles both immediate execution mode (non-pipeline) and queued execution mode (pipeline)
- Essential for maintaining connection state consistency across different query execution patterns
- Initializes result accumulation state for non-pipeline operations to prepare for incoming query results
- Enforces operational constraints such as preventing queries during COPY operations
- Foundation function that ensures all higher-level query operations start from a valid, consistent state

## Simplified Source
```c
static bool PQsendQueryStart(PGconn *conn, bool newQuery) {
    if (!conn)
        return false;

    // Clear error state for new query cycles (if no commands queued)
    if (newQuery && conn->cmd_queue_head == NULL)
        pqClearConnErrorState(conn);

    // Check connection is alive
    if (conn->status != CONNECTION_OK) {
        libpq_append_conn_error(conn, "no connection to the server");
        return false;
    }

    // Check not busy (unless queueing for pipeline)
    if (conn->asyncStatus != PGASYNC_IDLE &&
        conn->pipelineStatus == PQ_PIPELINE_OFF) {
        libpq_append_conn_error(conn, "another command is already in progress");
        return false;
    }

    if (conn->pipelineStatus != PQ_PIPELINE_OFF) {
        // Pipeline mode: check if safe to queue
        switch (conn->asyncStatus) {
            case PGASYNC_IDLE:
            case PGASYNC_PIPELINE_IDLE:
            case PGASYNC_READY:
            case PGASYNC_READY_MORE:
            case PGASYNC_BUSY:
                break; // OK to queue

            case PGASYNC_COPY_IN:
            case PGASYNC_COPY_OUT:
            case PGASYNC_COPY_BOTH:
                libpq_append_conn_error(conn, "cannot queue commands during COPY");
                return false;
        }
    } else {
        // Non-pipeline mode: initialize result state
        pqClearAsyncResult(conn);
        conn->partialResMode = false;
        conn->singleRowMode = false;
        conn->maxChunkSize = 0;
    }

    return true;
}
```