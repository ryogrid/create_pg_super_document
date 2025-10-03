# ExecAsyncNotify

## Location
[src/backend/executor/execAsync.c:88-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execAsync.c#L88-L116)

## Overview
Handles notification callbacks for asynchronous executor nodes when relevant events occur, serving as the event-driven response mechanism in PostgreSQL's asynchronous execution framework.

## Definition
```c
void ExecAsyncNotify(AsyncRequest *areq)
```

## Detailed Description
ExecAsyncNotify is called when the PostgreSQL wait event system detects that a previously configured file descriptor event has occurred. This function represents the "notify" phase of asynchronous execution and:

1. **Event Response Handling**: Responds to file descriptor events that were previously configured via ExecAsyncConfigureWait
2. **Instrumentation Management**: Provides performance monitoring for the notification and response processing
3. **Node Type Dispatching**: Routes the notification to the appropriate async-capable executor node handler
4. **Response Processing**: Immediately processes the asynchronous response after the node-specific notification handling
5. **Result Tracking**: Records execution statistics based on whether a tuple was successfully retrieved

This function is typically called from within PostgreSQL's event loop when the operating system signals that a file descriptor is ready for I/O operations, enabling efficient non-blocking data retrieval.

## Parameters / Member Variables
- `areq`: Pointer to AsyncRequest structure containing:
  - `requestee`: The target executor node that should handle the notification
  - `result`: Will be populated with the resulting tuple after notification processing
  - Event context and state information for the async operation

## Dependencies
- Functions called/Symbols referenced:
  - [InstrStartNode](../I/InstrStartNode.md): Starts performance instrumentation for the notification handling
  - nodeTag: Determines the executor node type for proper dispatching
  - [ExecAsyncForeignScanNotify](ExecAsyncForeignScanNotify.md): Handles notifications for foreign scan nodes
  - [ExecAsyncResponse](ExecAsyncResponse.md): Processes the asynchronous response after notification
  - [InstrStopNode](../I/InstrStopNode.md): Stops performance instrumentation and records tuple statistics
  - TupIsNull: Checks if the result tuple is null for instrumentation purposes
- Called from (representative examples):
  - [ExecAppendAsyncEventWait](ExecAppendAsyncEventWait.md): When processing events in Append node async execution

## Notes and Other Information
- Currently only supports T_ForeignScanState nodes; other node types will trigger an error
- This function bridges the gap between PostgreSQL's event system and executor node logic
- Provides its own instrumentation support since async operations may not follow standard execution timing patterns
- The instrumentation records actual tuple counts (1.0 or 0.0) based on whether a tuple was retrieved
- Part of the three-phase async execution model: Request → Configure Wait → Notify/Response
- Essential for implementing efficient event-driven I/O in PostgreSQL's foreign data wrapper architecture
- The function immediately calls ExecAsyncResponse, suggesting tight coupling between notification and response processing
- Enables PostgreSQL to efficiently handle multiple concurrent async operations through event-driven programming

## Simplified Source

```c
void
ExecAsyncNotify(AsyncRequest *areq)
{
    // Start performance instrumentation for notification handling
    if (areq->requestee->instrument)
        InstrStartNode(areq->requestee->instrument);

    // Dispatch to appropriate async-capable node type
    switch (nodeTag(areq->requestee)) {
        case T_ForeignScanState:
            ExecAsyncForeignScanNotify(areq);
            break;
        default:
            // Only foreign scan nodes currently support async
            elog(ERROR, "unrecognized node type: %d",
                 (int) nodeTag(areq->requestee));
    }

    // Process the async response
    ExecAsyncResponse(areq);

    // Stop instrumentation and record tuple statistics
    if (areq->requestee->instrument)
        InstrStopNode(areq->requestee->instrument,
                      TupIsNull(areq->result) ? 0.0 : 1.0);
}
```