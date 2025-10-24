# PQpipelineSync

## Location
[src/interfaces/libpq/fe-exec.c:3272-3281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3272-L3281)

## Overview
Sends a Sync message as part of a pipeline and immediately flushes the data to the server.

## Definition

```c
int
PQpipelineSync(PGconn *conn)
```
## Detailed Description
PQpipelineSync sends a Sync message to the PostgreSQL server as part of pipeline mode operation and immediately flushes the output buffer. This function is a wrapper around pqPipelineSyncInternal with immediate flushing enabled.

The Sync message serves as a synchronization point in pipeline mode, allowing the client to mark boundaries between batches of commands and ensure error recovery synchronization. When the server processes the Sync message, it sends back a ReadyForQuery message, confirming that all preceding commands in the pipeline have been processed.

The function validates that the connection is in pipeline mode, checks the async status, allocates a command queue entry, constructs and sends the Sync message, and immediately flushes the output buffer.

## Parameters / Member Variables
- `*conn`: The PostgreSQL connection in pipeline mode
## Dependencies
- Functions called/Symbols referenced:
  - [pqPipelineSyncInternal](../p/pqPipelineSyncInternal.md)

- Called from (representative examples):
  - [discardUntilSync](../d/discardUntilSync.md) (pgbench.c)
  - [executeMetaCommand](../e/executeMetaCommand.md) (pgbench.c)
  - [test_multi_pipelines](../t/test_multi_pipelines.md) (libpq_pipeline.c)
  - [test_pipeline_abort](../t/test_pipeline_abort.md) (libpq_pipeline.c)
  - [test_pipelined_insert](../t/test_pipelined_insert.md) (libpq_pipeline.c)
  - [test_prepared](../t/test_prepared.md) (libpq_pipeline.c)
  - [test_simple_pipeline](../t/test_simple_pipeline.md) (libpq_pipeline.c)
  - [test_transaction](../t/test_transaction.md) (libpq_pipeline.c)

## Notes and Other Information
- Returns 1 on success, 0 on failure
- Requires connection to be in pipeline mode (not PQ_PIPELINE_OFF)
- Immediately flushes output buffer unlike PQsendPipelineSync
- Creates a PGQUERY_SYNC entry in the command queue
- Critical for error recovery and batch boundary management in pipeline mode
- Cannot be called during COPY operations

## Simplified Source

```c
int PQpipelineSync(PGconn *conn) {
    // Send sync message with immediate flush enabled
    return pqPipelineSyncInternal(conn, true);
}
```