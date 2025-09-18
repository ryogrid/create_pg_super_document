# ExecInitGatherMerge

## Location
[src/backend/executor/nodeGatherMerge.c:67-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGatherMerge.c#L67-L182)

## Overview
ExecInitGatherMerge initializes the execution state for a GatherMerge plan node, setting up the infrastructure needed to collect and merge-sort tuples from parallel worker processes.

## Definition
```c
GatherMergeState *ExecInitGatherMerge(GatherMerge *node, EState *estate, int eflags)
```

## Detailed Description
This function creates and initializes a GatherMergeState structure that manages the execution of a GatherMerge operation. The GatherMerge node is responsible for collecting sorted results from multiple parallel worker processes and merging them into a single sorted output stream. Unlike regular Gather nodes, GatherMerge maintains the sort order of the combined results.

The function performs several key initialization tasks:
- Creates the GatherMergeState structure and links it to the execution infrastructure
- Initializes the outer plan (the plan that will be executed by workers)
- Sets up tuple descriptors and projection information for handling results
- Configures sort key information needed for the merge operation
- Allocates workspace memory for the gather merge process

The initialization ensures that the node can handle tuples both from the leader process (if need_to_scan_locally is true) and from worker processes via tuple queues, requiring flexible slot type handling.

## Parameters / Member Variables
- `node`: The GatherMerge plan node containing configuration such as sort columns, operators, and collations
- `estate`: The execution state containing global execution context and parameters
- `eflags`: Execution flags that may affect initialization behavior

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - ExecAssignExprContext
  - [ExecInitNode](ExecInitNode.md)
  - [ExecGetResultType](ExecGetResultType.md)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md)
  - [ExecConditionalAssignProjectionInfo](ExecConditionalAssignProjectionInfo.md)
  - PrepareSortSupportFromOrderingOp
  - [gather_merge_setup](../g/gather_merge_setup.md)
- Called from:
  - [ExecInitNode](ExecInitNode.md) (main executor initialization dispatcher)

## Notes and Other Information
- GatherMerge nodes do not support qual conditions as it is more efficient to apply them in child nodes
- The node does not have an inner plan, only an outer plan that gets executed by workers
- Sort key initialization includes setting up SortSupport structures but explicitly disables abbreviated key conversion for consistency with MergeAppend behavior
- The function sets outeropsset=true and outeropsfixed=false to handle the mixed tuple sources (leader vs workers)
- Memory allocation for sort keys and workspace is performed in the current memory context