# PQexitPipelineMode

## Location
src/interfaces/libpq/fe-exec.c: 3073 - 3141

## Overview
Ends pipeline mode and returns the connection to normal command mode, ensuring all results have been collected before exiting.

## Definition

```c
int
PQexitPipelineMode(PGconn *conn)
```
## Detailed Description
PQexitPipelineMode terminates pipeline mode on a PostgreSQL connection and transitions it back to normal command execution mode. The function performs comprehensive validation to ensure that it's safe to exit pipeline mode - all results must be collected, no operations can be in progress, and the connection must be in an appropriate state.

The function returns 1 on success (pipeline mode ended or connection wasn't in pipeline mode) and 0 on failure with an appropriate error message. It validates the connection state, checks for uncollected results, verifies no operations are busy, and ensures no pending commands remain in the queue.

## Parameters / Member Variables
- : The PostgreSQL connection handle to exit from pipeline mode

## Dependencies
- Functions called/Symbols referenced:
  - libpq_append_conn_error
  - pqFlush
  - PQ_PIPELINE_OFF
  - PGASYNC_IDLE
  - PGASYNC_PIPELINE_IDLE
  - PGASYNC_READY
  - PGASYNC_READY_MORE
  - PGASYNC_BUSY
  - PGASYNC_COPY_IN
  - PGASYNC_COPY_OUT
  - PGASYNC_COPY_BOTH

- Called from (representative examples):
  - readCommandResponse (pgbench.c)
  - discardUntilSync (pgbench.c)
  - test_disallowed_in_pipeline (libpq_pipeline.c)
  - test_multi_pipelines (libpq_pipeline.c)
  - test_simple_pipeline (libpq_pipeline.c)

## Notes and Other Information
- Returns 1 if already not in pipeline mode and connection is idle
- Prevents exiting pipeline mode when there are uncollected results
- Blocks exit during busy operations or COPY operations
- Flushes any pending output buffer data before completing the exit
- Sets connection status to PQ_PIPELINE_OFF and async status to PGASYNC_IDLE upon successful exit