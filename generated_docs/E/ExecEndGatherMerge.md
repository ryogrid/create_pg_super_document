# ExecEndGatherMerge

## Location
src/backend/executor/nodeGatherMerge.c: 284 - 296

## Overview
ExecEndGatherMerge performs cleanup for a GatherMerge node, freeing allocated storage and properly shutting down parallel workers and associated resources.

## Definition
```c
void ExecEndGatherMerge(GatherMergeState *node)
```

## Detailed Description
This function handles the orderly shutdown of a GatherMerge execution node. It follows the standard PostgreSQL executor pattern of cleaning up child nodes first, then shutting down the current node's specific resources.

The cleanup process involves two main steps:
1. **Child cleanup**: Calls ExecEndNode() on the outer plan state to ensure that child execution nodes are properly cleaned up first, following the bottom-up cleanup pattern used throughout the PostgreSQL executor.

2. **GatherMerge-specific cleanup**: Delegates to ExecShutdownGatherMerge() to handle the specialized cleanup required for parallel execution resources, including worker processes, tuple queues, shared memory, and parallel execution context.

This function is typically called during query termination, whether due to normal completion, error conditions, or user-initiated cancellation.

## Parameters / Member Variables
- `node`: The GatherMergeState containing the execution state to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - ExecEndNode
  - outerPlanState
  - ExecShutdownGatherMerge
- Called from:
  - ExecEndNode (main executor cleanup dispatcher)

## Notes and Other Information
- Follows the standard executor cleanup pattern of child-first cleanup
- The actual complexity of parallel resource cleanup is handled by ExecShutdownGatherMerge()
- This function is guaranteed to be called during query cleanup, even in error scenarios
- Cleanup order is important: child nodes must be cleaned up before parent nodes to maintain proper resource management
- The function is void as cleanup operations should not fail or throw errors that would prevent other cleanup from occurring