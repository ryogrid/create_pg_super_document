# ExecAsyncRequest

## Location
[src/backend/executor/execAsync.c:26-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execAsync.c#L26-L61)

## Overview
Asynchronously requests a tuple from a designated async-capable node in PostgreSQL's executor subsystem, handling the full asynchronous request lifecycle including parameter change detection and instrumentation.

## Definition
```c
void ExecAsyncRequest(AsyncRequest *areq)
```

## Detailed Description
ExecAsyncRequest is the primary entry point for initiating asynchronous tuple requests in PostgreSQL's query execution engine. This function coordinates the asynchronous execution workflow by:

1. **Parameter Change Detection**: Checks if any parameters have changed since the last execution and triggers a rescan if necessary
2. **Instrumentation Management**: Provides timing and performance measurement support for the asynchronous operation
3. **Node Type Dispatching**: Routes the request to the appropriate async-capable executor node based on its type
4. **Response Processing**: Handles the asynchronous response once the request is initiated
5. **Performance Tracking**: Stops instrumentation and records execution statistics

The function is designed to work with async-capable executor nodes that can handle non-blocking tuple requests, currently supporting ForeignScanState nodes for foreign data wrapper operations.

## Parameters / Member Variables
- `areq`: Pointer to AsyncRequest structure containing:
  - `requestee`: The target executor node to request a tuple from
  - `result`: Will contain the resulting tuple after the async operation
  - Associated state and context information for the async request

## Dependencies
- Functions called/Symbols referenced:
  - [ExecReScan](ExecReScan.md): Rescans the node if parameters have changed
  - [InstrStartNode](../I/InstrStartNode.md): Starts performance instrumentation
  - nodeTag: Gets the node type for dispatching
  - [ExecAsyncForeignScanRequest](ExecAsyncForeignScanRequest.md): Handles async requests for foreign scan nodes
  - [ExecAsyncResponse](ExecAsyncResponse.md): Processes the asynchronous response
  - [InstrStopNode](../I/InstrStopNode.md): Stops performance instrumentation and records stats
  - TupIsNull: Checks if the result tuple is null
- Called from (representative examples):
  - [ExecAppendAsyncBegin](ExecAppendAsyncBegin.md): When beginning async operations in Append nodes
  - [ExecAppendAsyncRequest](ExecAppendAsyncRequest.md): When requesting tuples in Append node async execution

## Notes and Other Information
- Currently only supports T_ForeignScanState nodes; other node types will trigger an error
- Provides its own instrumentation support since async operations may not follow standard execution patterns
- The function handles both the initiation and immediate response processing of async requests
- Parameter change detection ensures data consistency across async operations
- Part of PostgreSQL's asynchronous execution framework introduced to improve performance for foreign data wrappers and similar operations

## Simplified Source

```c
void
ExecAsyncRequest(AsyncRequest *areq)
{
    // Handle parameter changes by rescanning if needed
    if (areq->requestee->chgParam != NULL)
        ExecReScan(areq->requestee);

    // Start performance instrumentation
    if (areq->requestee->instrument)
        InstrStartNode(areq->requestee->instrument);

    // Dispatch to appropriate async-capable node type
    switch (nodeTag(areq->requestee)) {
        case T_ForeignScanState:
            ExecAsyncForeignScanRequest(areq);
            break;
        default:
            // Only foreign scan nodes currently support async
            elog(ERROR, "unrecognized node type: %d",
                 (int) nodeTag(areq->requestee));
    }

    // Process the async response
    ExecAsyncResponse(areq);

    // Stop instrumentation and record stats
    if (areq->requestee->instrument)
        InstrStopNode(areq->requestee->instrument,
                      TupIsNull(areq->result) ? 0.0 : 1.0);
}
```