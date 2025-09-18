# PQgetResult

## Location
src/interfaces/libpq/fe-exec.c: 2062 - 2223

## Overview
Core libpq function that retrieves the next PGresult from a query, handling both synchronous blocking and asynchronous non-blocking operation modes including pipeline processing.

## Definition


## Detailed Description
PQgetResult is a fundamental function in libpq that retrieves query results from the server. It operates in multiple modes depending on the connection state and pipeline configuration. The function first attempts to parse any available buffered data, then enters a blocking loop if necessary to wait for complete results.

In normal operation, the function handles various asynchronous states including BUSY (query in progress), READY (result available), and IDLE (query complete). For pipeline mode, it supports PIPELINE_IDLE state and PGRES_PIPELINE_SYNC results to manage batched queries efficiently.

The function performs I/O operations when needed, including flushing outbound data with pqFlush, waiting for socket readiness with pqWait, and reading incoming data with pqReadData. It handles error conditions gracefully by saving error states and transitioning to appropriate recovery states.

For COPY operations, the function delegates to getCopyResult to handle COPY_IN, COPY_OUT, and COPY_BOTH states. The function also supports chunked tuple processing (PGRES_TUPLES_CHUNK) for large result sets.

## Parameters / Member Variables
- : Pointer to the PGconn structure representing the database connection from which to retrieve results

## Dependencies
- Functions called/Symbols referenced:
  - parseInput
  - pqFlush
  - pqWait
  - pqReadData
  - pqSaveErrorResult
  - pqPrepareAsyncResult
  - pqSaveWriteError
  - pqPipelineProcessQueue
  - pqCommandQueueAdvance
  - getCopyResult
  - PQfireResultCreateEvents
- Called from (representative examples):
  - PQexecStart
  - PQexecFinish
  - libpqrcv_PQgetResult
  - consumeQueryResult
  - ExecQueryAndProcessResults

## Notes and Other Information
- Returns NULL when no more results are available or connection is invalid
- Blocks until a result is available unless the connection is in a non-blocking state
- In pipeline mode, returns NULL between queries and PGRES_PIPELINE_SYNC at pipeline end
- Handles both single query and pipeline query processing
- Manages complex state transitions between different asynchronous states
- Part of the core public libpq API for result retrieval
- Fires PGEVT_RESULTCREATE events for event-driven applications
- Essential for both synchronous and asynchronous query processing patterns