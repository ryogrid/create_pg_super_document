# PQpipelineStatus

## Location
[src/interfaces/libpq/fe-connect.c:7201-7209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7201-L7209)

## Overview
Returns the current pipeline mode status of a PostgreSQL connection, indicating whether pipeline mode is off, on, or aborted.

## Definition

```c
PGpipelineStatus
PQpipelineStatus(const PGconn *conn)
```
## Detailed Description
The PQpipelineStatus function queries the current pipeline mode status of a PostgreSQL connection. Pipeline mode is a libpq feature that allows batching multiple queries and processing their results efficiently. This function returns one of three possible states: pipeline mode is disabled (PQ_PIPELINE_OFF), pipeline mode is active (PQ_PIPELINE_ON), or pipeline mode is in an aborted state (PQ_PIPELINE_ABORTED) due to an error.

The function performs a simple null check on the connection pointer and returns PQ_PIPELINE_OFF if the connection is null, ensuring safe operation even with invalid connections.

## Parameters / Member Variables
- `*conn`: Pointer to the PGconn connection object whose pipeline status is to be queried. If NULL, the function safely returns PQ_PIPELINE_OFF.
## Dependencies
- Functions called/Symbols referenced:
  - PQ_PIPELINE_OFF (enum constant)
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md) (pgbench.c)
  - [executeMetaCommand](../e/executeMetaCommand.md) (pgbench.c)
  - [test_disallowed_in_pipeline](../t/test_disallowed_in_pipeline.md) (libpq_pipeline.c)
  - [test_pipeline_abort](../t/test_pipeline_abort.md) (libpq_pipeline.c)
  - [test_simple_pipeline](../t/test_simple_pipeline.md) (libpq_pipeline.c)

## Notes and Other Information
- The return type PGpipelineStatus is an enum with three possible values: PQ_PIPELINE_OFF, PQ_PIPELINE_ON, and PQ_PIPELINE_ABORTED
- This function is extensively used in PostgreSQL's testing infrastructure, particularly in libpq_pipeline tests and pgbench
- Pipeline mode is a performance optimization feature that allows sending multiple queries without waiting for individual results
- The function is declared in libpq-fe.h and implemented in fe-connect.c at lines 7201-7209