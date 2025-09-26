# PQsendPipelineSync

## Location
src/interfaces/libpq/fe-exec.c: 3282 - 3293

## Overview
Sends a Sync message as part of a pipeline without immediately flushing the data to the server.

## Definition


## Detailed Description
PQsendPipelineSync sends a Sync message to the PostgreSQL server as part of pipeline mode operation but does not immediately flush the output buffer. This function is a wrapper around pqPipelineSyncInternal with immediate flushing disabled.

Unlike PQpipelineSync, this function allows for batching multiple commands and sync messages before sending them to the server, potentially improving performance by reducing the number of network round trips. The data will be flushed when the output buffer reaches a threshold size or when an explicit flush is performed.

The Sync message serves as a synchronization point in pipeline mode, marking boundaries between batches of commands and ensuring proper error recovery semantization when the server processes it.

## Parameters / Member Variables
- : The PostgreSQL connection in pipeline mode

## Dependencies
- Functions called/Symbols referenced:
  - pqPipelineSyncInternal

- Called from (representative examples):
  - executeMetaCommand (pgbench.c)
  - test_multi_pipelines (libpq_pipeline.c)

## Notes and Other Information
- Returns 1 on success, 0 on failure
- Requires connection to be in pipeline mode (not PQ_PIPELINE_OFF)
- Does not immediately flush output buffer, unlike PQpipelineSync
- Uses conditional flushing based on buffer size thresholds via pqPipelineFlush
- Creates a PGQUERY_SYNC entry in the command queue
- Allows for better batching performance in pipeline mode
- Cannot be called during COPY operations
- Data will eventually be sent when buffer fills up or explicit flush occurs