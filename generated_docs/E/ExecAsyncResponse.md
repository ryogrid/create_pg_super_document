# ExecAsyncResponse

## Location
[src/backend/executor/execAsync.c:117-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execAsync.c#L117-L136)

## Overview
Dispatches asynchronous response handling to the appropriate requestor node when an async-capable executor node has produced a result, serving as the callback mechanism in PostgreSQL's asynchronous execution framework.

## Definition
```c
void ExecAsyncResponse(AsyncRequest *areq)
```

## Detailed Description
ExecAsyncResponse acts as the response dispatcher in PostgreSQL's asynchronous execution system, routing completed async operations back to the nodes that initiated them. This function:

1. **Requestor Identification**: Determines the type of node that originally requested the asynchronous operation
2. **Response Dispatching**: Routes the response to the appropriate node-specific handler based on the requestor's node type
3. **Result Delivery**: Ensures that the asynchronous result is properly delivered to the requesting executor node

Unlike the other async functions (ExecAsyncRequest, ExecAsyncConfigureWait, ExecAsyncNotify) which operate on the "requestee" (the node being asked to perform work), this function operates on the "requestor" (the node that initiated the async request and needs to receive the result).

This function is called from both ExecAsyncRequest and ExecAsyncNotify, indicating it's used in both immediate response scenarios and event-driven response scenarios.

## Parameters / Member Variables
- `areq`: Pointer to AsyncRequest structure containing:
  - `requestor`: The executor node that originally initiated the async request and should receive the response
  - `requestee`: The node that performed the async work (not directly used in this function)
  - `result`: The tuple result from the async operation that needs to be delivered

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag: Determines the requestor node type for proper response dispatching
  - [ExecAsyncAppendResponse](ExecAsyncAppendResponse.md): Handles response delivery for Append executor nodes
- Called from (representative examples):
  - [ExecAsyncRequest](ExecAsyncRequest.md): For immediate response processing after async request initiation
  - [ExecAsyncNotify](ExecAsyncNotify.md): For event-driven response processing when async operations complete

## Notes and Other Information
- Currently only supports T_AppendState nodes as requestors; other node types will trigger an error
- This function completes the async execution cycle by delivering results back to the initiating node
- Does not provide its own instrumentation support, relying on the calling functions for performance monitoring
- The function's simplicity reflects its role as a pure dispatcher rather than a complex executor
- Essential for maintaining the async execution contract where results are properly delivered to requestors
- Part of the asynchronous execution framework that enables efficient parallel processing in operations like Append node execution
- The distinction between requestor and requestee is crucial for understanding the async execution flow in PostgreSQL

## Simplified Source

```c
void ExecAsyncResponse(AsyncRequest *areq) {
    // Dispatch response to appropriate handler based on requestor type
    switch (nodeTag(areq->requestor)) {
        case T_AppendState:
            ExecAsyncAppendResponse(areq);
            break;
        default:
            // Error for unsupported node types
            elog(ERROR, "unrecognized node type: %d",
                 (int) nodeTag(areq->requestor));
    }
}
```