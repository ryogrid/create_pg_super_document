# ExecShutdownGatherMergeWorkers

## Location
[src/backend/executor/nodeGatherMerge.c:316-333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGatherMerge.c#L316-L333)

## Overview
ExecShutdownGatherMergeWorkers stops all parallel workers associated with a GatherMerge node and cleans up worker-specific resources.

## Definition
```c
static void ExecShutdownGatherMergeWorkers(GatherMergeState *node)
```

## Detailed Description
This function handles the worker-specific cleanup phase of GatherMerge shutdown. It focuses on terminating active worker processes and cleaning up the local resources used for worker communication and management.

The cleanup process involves two main operations:
1. **Worker termination**: Calls ExecParallelFinish() to signal all worker processes to terminate gracefully and waits for their completion. This function handles the coordination of worker shutdown, including sending termination signals and cleaning up inter-process communication channels.

2. **Local resource cleanup**: Frees the local array of TupleQueueReader pointers that was used to manage communication with worker processes. This array is dynamically allocated during worker initialization and contains references to tuple queue readers for each active worker.

The function is designed to be safe even when called multiple times or when workers were never successfully initialized, making it suitable for use in error recovery scenarios.

## Parameters / Member Variables
- `node`: The GatherMergeState containing worker state to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [ExecParallelFinish](ExecParallelFinish.md)
  - [pfree](../p/pfree.md)
- Called from:
  - [ExecShutdownGatherMerge](ExecShutdownGatherMerge.md) (during normal shutdown)
  - [ExecReScanGatherMerge](ExecReScanGatherMerge.md) (during query rescanning operations)

## Notes and Other Information
- The function is declared static as it is only used internally within the nodeGatherMerge.c file
- Safely handles cases where parallel execution was never initialized (pei == NULL)
- The reader array cleanup prevents memory leaks by freeing the dynamically allocated array of reader pointers
- After cleanup, the reader pointer is set to NULL to prevent accidental reuse
- This function is part of the layered cleanup approach where worker-specific resources are cleaned up before the overall parallel context
- Can be called during both normal termination and error recovery without causing issues
- The cleanup is idempotent - multiple calls will not cause problems or double-free errors