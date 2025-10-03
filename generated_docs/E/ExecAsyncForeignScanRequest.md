# ExecAsyncForeignScanRequest

## Location
[src/backend/executor/nodeForeignscan.c:456-471](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeForeignscan.c#L456-L471)

## Overview
Initiates an asynchronous request to fetch a tuple from a foreign data wrapper that supports asynchronous operations, allowing for non-blocking data retrieval from external data sources.

## Definition
```c
void ExecAsyncForeignScanRequest(AsyncRequest *areq)
```

## Detailed Description
This function is part of PostgreSQL's asynchronous execution framework for foreign scans. It serves as a bridge between the generic asynchronous request infrastructure and FDW-specific asynchronous implementations. When called, it extracts the ForeignScanState from the async request structure and delegates the actual asynchronous request handling to the FDW's ForeignAsyncRequest callback function.

The function assumes that the FDW supports asynchronous operations and has provided a ForeignAsyncRequest callback. This enables PostgreSQL to perform concurrent operations while waiting for data from potentially slow external sources, improving overall query performance in scenarios involving multiple foreign scans or mixed local/foreign operations.

## Parameters / Member Variables
- `areq`: A pointer to an AsyncRequest structure containing the request details and the target ForeignScanState node (accessible via areq->requestee)

## Dependencies
- Functions called/Symbols referenced:
  - [AsyncRequest](../A/AsyncRequest.md) (structure)
  - [ForeignScanState](../F/ForeignScanState.md) (structure) 
  - [FdwRoutine](../F/FdwRoutine.md) (structure)
  - ForeignAsyncRequest (FDW callback function)
- Called from (representative examples):
  - [ExecAsyncRequest](ExecAsyncRequest.md) (in execAsync.c)

## Notes and Other Information
- The function contains an assertion that the FDW's ForeignAsyncRequest callback is not NULL, indicating this should only be called for async-capable FDWs
- Part of the asynchronous foreign scan infrastructure that enables concurrent tuple fetching
- The actual implementation of asynchronous behavior is delegated to the specific FDW
- Located in src/backend/executor/nodeForeignscan.c:456-471

## Simplified Source

```c
void ExecAsyncForeignScanRequest(AsyncRequest *areq) {
    ForeignScanState *node = (ForeignScanState *) areq->requestee;
    FdwRoutine *fdwroutine = node->fdwroutine;

    // Ensure FDW supports async operations
    Assert(fdwroutine->ForeignAsyncRequest != NULL);

    // Delegate to FDW-specific async request handler
    fdwroutine->ForeignAsyncRequest(areq);
}
```