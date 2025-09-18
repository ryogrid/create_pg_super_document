# AsyncRequest

## Location
[src/include/nodes/execnodes.h:604-613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L604-L613)

## Overview
AsyncRequest is a struct that manages state for asynchronous tuple requests between executor nodes in PostgreSQL's query execution system.

## Definition


## Detailed Description
AsyncRequest encapsulates the state necessary for asynchronous communication between executor nodes, primarily used in parallel query execution scenarios like Append nodes with foreign tables. It enables non-blocking tuple retrieval by allowing requestor nodes to issue requests and later check for completion, facilitating better resource utilization during I/O-bound operations.

## Parameters / Member Variables
- : Pointer to the PlanState node that is requesting tuples
- : Pointer to the PlanState node that will provide the tuples  
- : Integer scratch space available for the requestor's use
- : Boolean flag indicating whether a callback notification is needed
- : Boolean flag indicating whether the request has been completed and the result is valid
- : Pointer to TupleTableSlot containing the result tuple, or NULL/empty slot if no more tuples are available

## Dependencies
- Functions called/Symbols referenced:
  - [PlanState](../P/PlanState.md) (struct type)
  - TupleTableSlot (struct type)
- Called from (representative examples):
  - ExecAsyncRequest (src/backend/executor/execAsync.c:26)
  - [ExecAppendAsyncRequest](../E/ExecAppendAsyncRequest.md) (src/backend/executor/nodeAppend.c:992)
  - [ExecAsyncForeignScanRequest](../E/ExecAsyncForeignScanRequest.md) (src/backend/executor/nodeForeignscan.c:456)

## Notes and Other Information
This structure is primarily used in PostgreSQL's asynchronous execution framework, particularly for foreign data wrappers and parallel append operations. The asynchronous request mechanism helps optimize performance by allowing the executor to overlap computation with I/O operations, especially beneficial when dealing with remote data sources or parallel worker processes.