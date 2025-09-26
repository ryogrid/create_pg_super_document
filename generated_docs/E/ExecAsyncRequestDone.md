# ExecAsyncRequestDone

## Location
src/backend/executor/execAsync.c: 137 - 148

## Overview
Marks an asynchronous request as complete and stores the result tuple, serving as the completion notification mechanism for async-capable executor nodes to signal successful tuple delivery.

## Definition
```c
void ExecAsyncRequestDone(AsyncRequest *areq, TupleTableSlot *result)
```

## Detailed Description
ExecAsyncRequestDone is a utility function called by requestee nodes (the nodes performing asynchronous work) to signal that an async operation has completed and to deliver the resulting tuple. This function:

1. **Completion Marking**: Sets the request_complete flag to true, indicating that the asynchronous operation has finished
2. **Result Storage**: Stores the resulting TupleTableSlot in the AsyncRequest structure for later retrieval by the requestor
3. **State Synchronization**: Provides a clean interface for async nodes to communicate completion status

This function is designed to be called from within the asynchronous node's callback functions (either ExecAsyncRequest or ExecAsyncNotify callbacks) when the node has successfully retrieved or produced a tuple. It represents the final step in the async execution chain from the requestee's perspective.

## Parameters / Member Variables
- `areq`: Pointer to AsyncRequest structure that will be updated with completion status
  - `request_complete`: Will be set to true to indicate completion
  - `result`: Will be set to the provided result tuple
- `result`: TupleTableSlot containing the tuple produced by the async operation (may be NULL if no tuple was produced)

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a simple state-setting function)
- Called from (representative examples):
  - Typically called from within FDW (Foreign Data Wrapper) callback implementations
  - May be called from ExecAsyncRequest or ExecAsyncNotify callback functions of specific node types

## Notes and Other Information
- This is a simple state-setting function with no complex logic or error handling
- The function does not perform any validation on the input parameters
- It's the responsibility of the calling async node to ensure the result tuple is properly formatted
- The function enables async nodes to signal both successful completion (with a tuple) and unsuccessful completion (with NULL result)
- Part of the contract between async-capable nodes and the async execution framework
- The completion flag allows the async framework to determine when operations have finished without polling
- This function complements the other async execution functions by providing the completion mechanism
- Essential for implementing foreign data wrappers and other async-capable executor nodes in PostgreSQL