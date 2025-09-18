# ExecShutdownGatherMerge

## Location
src/backend/executor/nodeGatherMerge.c: 297 - 315

## Overview
ExecShutdownGatherMerge destroys the parallel execution setup for GatherMerge nodes, including worker processes and the parallel context.

## Definition
```c
void ExecShutdownGatherMerge(GatherMergeState *node)
```

## Detailed Description
This function performs the complete shutdown sequence for parallel execution resources associated with a GatherMerge node. It coordinates the orderly termination of all parallel processing components to ensure clean resource cleanup.

The shutdown process follows a two-phase approach:
1. **Worker shutdown**: Calls ExecShutdownGatherMergeWorkers() to handle the termination of individual worker processes, including stopping workers, cleaning up tuple queues, and deallocating worker-specific resources.

2. **Parallel context cleanup**: Destroys the overall parallel execution context via ExecParallelCleanup(), which handles shared memory cleanup, inter-process communication teardown, and other global parallel execution resources.

This function is designed to be safe to call multiple times and in error conditions, ensuring that resources are properly cleaned up regardless of how the query terminates.

## Parameters / Member Variables
- `node`: The GatherMergeState containing the parallel execution infrastructure to shut down

## Dependencies
- Functions called/Symbols referenced:
  - [ExecShutdownGatherMergeWorkers](ExecShutdownGatherMergeWorkers.md)
  - [ExecParallelCleanup](ExecParallelCleanup.md)
- Called from:
  - [ExecEndGatherMerge](ExecEndGatherMerge.md) (during normal query cleanup)
  - [ExecShutdownNode_walker](ExecShutdownNode_walker.md) (during emergency shutdown scenarios)

## Notes and Other Information
- The shutdown sequence is carefully ordered: workers are shut down before the parallel context to ensure proper cleanup
- The function safely handles cases where parallel execution was never initialized (pei == NULL)
- After cleanup, the pei field is set to NULL to prevent double-cleanup attempts
- This function is part of the PostgreSQL executor's resource management system and must be robust against failures
- Called during both normal query completion and error recovery scenarios
- The cleanup is designed to be idempotent - multiple calls are safe and will not cause issues