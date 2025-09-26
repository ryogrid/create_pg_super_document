# ExecAsyncRequest

## Location
src/backend/executor/execAsync.c: 26 - 61

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
  - ExecReScan: Rescans the node if parameters have changed
  - InstrStartNode: Starts performance instrumentation
  - nodeTag: Gets the node type for dispatching
  - ExecAsyncForeignScanRequest: Handles async requests for foreign scan nodes
  - ExecAsyncResponse: Processes the asynchronous response
  - InstrStopNode: Stops performance instrumentation and records stats
  - TupIsNull: Checks if the result tuple is null
- Called from (representative examples):
  - ExecAppendAsyncBegin: When beginning async operations in Append nodes
  - ExecAppendAsyncRequest: When requesting tuples in Append node async execution

## Notes and Other Information
- Currently only supports T_ForeignScanState nodes; other node types will trigger an error
- Provides its own instrumentation support since async operations may not follow standard execution patterns
- The function handles both the initiation and immediate response processing of async requests
- Parameter change detection ensures data consistency across async operations
- Part of PostgreSQL's asynchronous execution framework introduced to improve performance for foreign data wrappers and similar operations