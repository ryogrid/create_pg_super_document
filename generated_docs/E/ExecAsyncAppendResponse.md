# ExecAsyncAppendResponse

## Location
[src/backend/executor/nodeAppend.c:1127-1172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAppend.c#L1127-L1172)

## Overview
Receives and processes a response from an asynchronous request made by an Append node executor.

## Definition
```c
void ExecAsyncAppendResponse(AsyncRequest *areq)
```

## Detailed Description
This function handles responses from asynchronous subplan execution requests within an Append node context. It processes the result returned by an async subplan, managing the completion state and result storage. When a subplan completes with a valid tuple, the function saves the result and marks the subplan as ready for new requests. For empty results or NULL slots, it decrements the count of remaining async operations.

The function is part of PostgreSQL's asynchronous execution framework, allowing Append nodes to execute multiple subplans concurrently and collect their results as they become available.

## Parameters / Member Variables
- `areq`: AsyncRequest structure containing the asynchronous request details, including the requestor (AppendState), result slot, completion status, and callback state

## Dependencies
- Functions called/Symbols referenced:
  - TupIsNull
  - [bms_add_member](../b/bms_add_member.md)
  - IsA (assertion macro)
  - Assert (assertion macro)
- Called from (representative examples):
  - ExecAsyncResponse

## Notes and Other Information
- The function performs extensive assertion checks to validate the request state and result types
- Handles three main cases: pending requests (early return), empty/NULL results (decrements async count), and valid results (stores result and marks subplan for new requests)
- Does not immediately launch new requests for completed subplans to avoid potential immediate completion conflicts  
- The result must be either NULL or a valid TupleTableSlot
- Updates the AppendState tracking counters and bitmaps to manage async subplan states
- Part of the broader async execution infrastructure that enables concurrent subplan execution in Append nodes