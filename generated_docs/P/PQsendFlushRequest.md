# PQsendFlushRequest

## Location
[src/interfaces/libpq/fe-exec.c:3371-3410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3371-L3410)

## Overview
PQsendFlushRequest sends a flush request to the PostgreSQL server to force it to flush its output buffer, particularly useful in pipeline mode when a sync point is not desired.

## Definition

```c
int
PQsendFlushRequest(PGconn *conn)
```
## Detailed Description
This function sends a Flush message to the PostgreSQL server using the wire protocol, instructing the server to flush any buffered output without establishing a synchronization point. This is particularly valuable in pipeline mode scenarios where you want to ensure data is sent to the client without the overhead and semantics of a full pipeline sync operation.

The function performs several validation steps before sending the flush request:
- Verifies the connection is valid and in CONNECTION_OK status
- Ensures no other command is in progress (unless in pipeline mode where queueing is allowed)
- Constructs and sends a Flush protocol message
- Uses pipeline-aware flushing to manage when data is actually transmitted

Unlike pipeline sync operations, flush requests do not create synchronization boundaries and do not affect the pipeline's command queue structure.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle that must be in a valid connected state
## Dependencies
- Functions called/Symbols referenced:
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - [pqPutMsgStart](../p/pqPutMsgStart.md)
  - [pqPutMsgEnd](../p/pqPutMsgEnd.md)
  - [pqPipelineFlush](../p/pqPipelineFlush.md)
- Called from:
  - [test_nosync](../t/test_nosync.md) (libpq_pipeline.c:654)
  - [test_pipeline_idle](../t/test_pipeline_idle.md) (libpq_pipeline.c:1437, 1457, 1478)
  - [test_singlerowmode](../t/test_singlerowmode.md) (libpq_pipeline.c:1688, 1711, 1730)
  - [test_uniqviol](../t/test_uniqviol.md) (libpq_pipeline.c:2052)

## Notes and Other Information
- This is a public libpq API function available to client applications
- Returns 1 on success, 0 on failure
- The function is designed to work both in regular mode and pipeline mode
- In pipeline mode, it allows queueing flush requests even when other commands are active
- Uses PqMsg_Flush message type in the PostgreSQL wire protocol
- Does not add entries to the command queue like sync operations do
- Particularly useful for forcing server output in interactive or streaming scenarios
- The function is located at src/interfaces/libpq/fe-exec.c:3371-3410