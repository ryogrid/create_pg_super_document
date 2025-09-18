# ExecAppendAsyncGetNext

## Location
[src/backend/executor/nodeAppend.c:914-962](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAppend.c#L914-L962)

## Overview
Retrieves the next tuple from any of the active asynchronous subplans in an Append node, coordinating between async requests and event waiting to efficiently collect results from concurrent operations.

## Definition


## Detailed Description
This function implements the core tuple retrieval logic for asynchronous Append execution, providing a unified interface to collect results from multiple concurrently executing subplans. It manages the complex coordination between asynchronous request processing and event waiting to maximize throughput while maintaining correct execution semantics.

The function operates in a polling loop that:
1. **Initial Request**: Attempts to immediately retrieve a tuple from any completed async operation
2. **Event Loop**: If no tuple is immediately available, enters a waiting loop that polls for async events and processes completed operations
3. **Interruption Handling**: Checks for query cancellation and other interrupts during waiting
4. **Sync Integration**: Coordinates with synchronous subplan execution by breaking from the async loop when sync subplans require attention

The function returns true when a tuple is available (including end-of-scan indication) and false when control should return to synchronous subplan processing. This design allows for seamless integration between async and sync execution modes within the same Append node.

## Parameters / Member Variables
- : Pointer to AppendState containing async execution state and remaining async subplan tracking
- : Output parameter that receives the retrieved tuple slot or NULL if no tuple is available

## Dependencies
- Functions called/Symbols referenced:
  - [ExecAppendAsyncRequest](ExecAppendAsyncRequest.md) (async tuple request processing)
  - [ExecAppendAsyncEventWait](ExecAppendAsyncEventWait.md) (async event waiting and processing)
  - CHECK_FOR_INTERRUPTS (query interruption handling)
  - ExecClearTuple (tuple slot clearing for end-of-scan)
- Called from (representative examples):
  - [ExecAppend](ExecAppend.md) (main append execution function)

## Notes and Other Information
- Only called when async subplans are available and active (as_nasyncremain > 0)
- Integrates query interruption checking to support responsive query cancellation
- Returns cleared tuple slot when all async operations and sync subplans are complete
- Critical for performance in mixed sync/async execution scenarios
- Part of PostgreSQL's non-blocking I/O infrastructure for improved concurrency
- Enables efficient processing of foreign tables, parallel queries, and partitioned table scans
- The polling loop design balances CPU usage with responsiveness to completed async operations