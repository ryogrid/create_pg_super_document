# pqAppendCmdQueueEntry

## Location
[src/interfaces/libpq/fe-exec.c:1339-1385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L1339-L1385)

## Overview
Appends a caller-allocated command queue entry to the connection's command queue and updates the connection's asynchronous status to properly handle pipeline execution states.

## Definition
```c
static void pqAppendCmdQueueEntry(PGconn *conn, PGcmdQueueEntry *entry)
```

## Detailed Description
This function adds a pre-allocated command queue entry to the tail of the connection's command queue and manages the connection's asynchronous status based on the current pipeline state. The function handles three distinct pipeline states:

1. **PQ_PIPELINE_OFF/PQ_PIPELINE_ON**: In normal operation, if the connection is idle, it transitions to busy state to wait for server responses. If there are already results ready to consume, the status remains unchanged.

2. **PQ_PIPELINE_ABORTED**: In aborted pipeline state, no queries are sent to the server, so if the connection is idle or pipeline-idle, it triggers queue processing to handle queued commands locally.

The function maintains the integrity of the command queue as a linked list by properly linking the new entry to the tail and updating both head and tail pointers as needed.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection object containing the command queue
- `entry`: Pointer to the command queue entry to be appended (must have next set to NULL)

## Dependencies
- Functions called/Symbols referenced:
  - pqPipelineProcessQueue
  - Assert (debugging macro)
  - PQ_PIPELINE_OFF, PQ_PIPELINE_ON, PQ_PIPELINE_ABORTED (pipeline status constants)
  - PGASYNC_IDLE, PGASYNC_BUSY, PGASYNC_PIPELINE_IDLE (async status constants)
- Called from (representative examples):
  - [PQsendQueryInternal](../P/PQsendQueryInternal.md)
  - [PQsendPrepare](../P/PQsendPrepare.md)
  - [PQsendQueryGuts](../P/PQsendQueryGuts.md)
  - [PQsendTypedCommand](../P/PQsendTypedCommand.md)
  - [pqPipelineSyncInternal](pqPipelineSyncInternal.md)

## Notes and Other Information
- This is a static function, only accessible within fe-exec.c
- The caller must ensure the entry's next pointer is NULL before calling this function
- The query data must already be placed in the output buffer before calling this function
- Critical for maintaining proper command ordering in PostgreSQL's pipeline mode
- Handles state transitions that ensure proper asynchronous query execution flow
- The function assumes the entry is properly initialized and ready for queue insertion