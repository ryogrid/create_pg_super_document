# ExecAsyncRequestPending

## Location
src/backend/executor/execAsync.c: 149 - 154

## Overview
Indicates that an async-capable node is pending for a callback and not yet ready to return a tuple result.

## Definition


## Detailed Description
ExecAsyncRequestPending is a utility function used by requestee nodes (async-capable nodes) in PostgreSQL's asynchronous execution framework to signal that an asynchronous tuple request is still pending and requires a callback to complete. When called, it sets up the AsyncRequest structure to indicate that:

1. A callback is needed (callback_pending = true)
2. The request is not yet complete (request_complete = false) 
3. No result is available yet (result = NULL)

This function is typically called from within a node's ExecAsyncRequest or ExecAsyncNotify callback methods when the node cannot immediately return a result and needs to wait for some asynchronous operation (like I/O) to complete.

The asynchronous execution framework allows nodes to request tuples from other nodes without blocking. Instead of waiting synchronously, requestor nodes can continue processing other work while async-capable requestee nodes handle requests in the background.

## Parameters / Member Variables
- : Pointer to the AsyncRequest structure that tracks the state of the asynchronous tuple request. This structure contains information about the requestor node, requestee node, and the current status of the request.

## Dependencies
- Functions called/Symbols referenced:
  - [AsyncRequest](../A/AsyncRequest.md) (struct type from execnodes.h)
- Called from (representative examples):
  - Async-capable node implementations (e.g., ForeignScan nodes)
  - ExecAsyncRequest callbacks
  - ExecAsyncNotify callbacks

## Notes and Other Information
- This function is part of PostgreSQL's asynchronous execution framework introduced for better performance with parallel operations and foreign data wrappers
- Should only be called by requestee nodes that support asynchronous operation
- Alternative to ExecAsyncRequestDone, which is used when a result is immediately available
- The callback_pending flag set by this function indicates to the executor that the node will need another callback (ExecAsyncNotify) when the underlying asynchronous operation completes
- Typically used in conjunction with file descriptor event handling for I/O operations