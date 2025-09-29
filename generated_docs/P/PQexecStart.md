# PQexecStart

## Location
[src/interfaces/libpq/fe-exec.c:2344-2409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2344-L2409)

## Overview
Internal preparation function that validates connection state and clears any pending results before executing synchronous commands in libpq.

## Definition
```c
static bool PQexecStart(PGconn *conn)
```

## Detailed Description
PQexecStart is a common preparation function used by all synchronous libpq execution functions (PQexec, PQexecParams, PQprepare, PQexecPrepared, etc.). It performs essential validation and cleanup tasks to ensure the connection is ready for a new command execution cycle.

The function handles several critical tasks: validates the connection object, manages error state appropriately for pipeline vs. normal mode, prevents synchronous commands in pipeline mode, and cleans up any residual results from previous operations. It also handles special cases for COPY operations that may be in progress.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object to prepare for command execution

## Dependencies
- Functions called/Symbols referenced:
  - pqClearConnErrorState
  - PQ_PIPELINE_OFF
  - [PQgetResult](PQgetResult.md)
  - ExecStatusType
  - PGRES_COPY_IN
  - [PQputCopyEnd](PQputCopyEnd.md)
  - [libpq_gettext](../l/libpq_gettext.md)
  - PGRES_COPY_OUT
  - PGASYNC_BUSY
  - PGRES_COPY_BOTH
  - CONNECTION_BAD
- Called from (representative examples):
  - [PQexec](PQexec.md)
  - [PQexecParams](PQexecParams.md)
  - [PQprepare](PQprepare.md)
  - [PQexecPrepared](PQexecPrepared.md)
  - [PQdescribePrepared](PQdescribePrepared.md)
  - [PQdescribePortal](PQdescribePortal.md)
  - [PQclosePrepared](PQclosePrepared.md)
  - [PQclosePortal](PQclosePortal.md)

## Notes and Other Information
- Returns false if the connection is invalid or cannot be prepared for execution
- Prevents synchronous command execution when the connection is in pipeline mode
- Automatically discards any unprocessed results from previous operations for backward compatibility
- Handles COPY IN states by sending a termination message to the server
- Handles COPY OUT states by switching connection to busy state and allowing data to be discarded
- Blocks COPY BOTH operations as they are incompatible with synchronous execution
- Clears connection error state only when no commands are queued in pipeline mode
- Essential for maintaining connection state consistency across all synchronous libpq operations

## Simplified Source

```c
static bool PQexecStart(PGconn *conn) {
    PGresult *result;

    // Basic connection validation
    if (!conn)
        return false;

    // Clear error state if no commands queued
    if (conn->cmd_queue_head == NULL)
        pqClearConnErrorState(conn);

    // Prevent synchronous commands in pipeline mode
    if (conn->pipelineStatus != PQ_PIPELINE_OFF) {
        libpq_append_conn_error(conn, "synchronous command execution functions are not allowed in pipeline mode");
        return false;
    }

    // Clean up any pending results from previous operations
    while ((result = PQgetResult(conn)) != NULL) {
        ExecStatusType resultStatus = result->resultStatus;
        PQclear(result);

        // Handle COPY operations specially
        if (resultStatus == PGRES_COPY_IN) {
            // Terminate COPY IN operation
            if (PQputCopyEnd(conn, libpq_gettext("COPY terminated by new PQexec")) < 0)
                return false;
        } else if (resultStatus == PGRES_COPY_OUT) {
            // Switch to busy state, discard remaining data
            conn->asyncStatus = PGASYNC_BUSY;
        } else if (resultStatus == PGRES_COPY_BOTH) {
            // COPY BOTH not allowed with PQexec
            libpq_append_conn_error(conn, "PQexec not allowed during COPY BOTH");
            return false;
        }

        // Check for connection loss
        if (conn->status == CONNECTION_BAD)
            return false;
    }

    return true;
}
```