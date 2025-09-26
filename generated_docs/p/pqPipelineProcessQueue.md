# pqPipelineProcessQueue

## Location
src/interfaces/libpq/fe-exec.c: 3180 - 3271

## Overview
Processes the next query in the pipeline queue, managing state transitions and error handling for pipelined command execution.

## Definition


## Detailed Description
pqPipelineProcessQueue is a subroutine for PQgetResult that manages the processing of queued commands in pipeline mode. It handles state transitions between different async states, processes the next command in the queue when appropriate, and manages special cases like aborted pipelines.

The function first checks if the connection is in a state where it can process the next query (not busy with current operations). It then transitions the connection to the appropriate state, resets result accumulation modes, and either prepares for normal query processing or handles aborted pipeline scenarios by generating PGRES_PIPELINE_ABORTED results.

## Parameters / Member Variables
- : The PostgreSQL connection containing the command queue to process

## Dependencies
- Functions called/Symbols referenced:
  - pqClearConnErrorState
  - pqClearAsyncResult
  - PQmakeEmptyPGresult
  - libpq_append_conn_error
  - pqSaveErrorResult
  - PGASYNC_COPY_IN, PGASYNC_COPY_OUT, PGASYNC_COPY_BOTH
  - PGASYNC_READY, PGASYNC_READY_MORE, PGASYNC_BUSY
  - PGASYNC_IDLE, PGASYNC_PIPELINE_IDLE
  - PQ_PIPELINE_OFF, PQ_PIPELINE_ABORTED
  - PGQUERY_SYNC
  - PGRES_PIPELINE_ABORTED

- Called from (representative examples):
  - pqAppendCmdQueueEntry (fe-exec.c)
  - PQgetResult (fe-exec.c)

## Notes and Other Information
- Only processes when connection is idle or in pipeline-idle state
- Resets partial result modes (partialResMode, singleRowMode, maxChunkSize) for each new query
- Transitions to PGASYNC_IDLE when command queue is empty
- Generates PGRES_PIPELINE_ABORTED results for non-SYNC commands in aborted pipelines
- Prepares connection state for next query processing by clearing error state and async results
- Critical component of PostgreSQL's pipeline mode implementation