# ExecEndGather

## Location
[src/backend/executor/nodeGather.c:244-255](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGather.c#L244-L255)

## Overview
Cleans up and frees all resources allocated by a Gather plan node, including shutting down parallel workers and releasing the parallel execution context.

## Definition

```c
void
ExecEndGather(GatherState *node)
```
## Detailed Description
ExecEndGather performs the cleanup phase for Gather plan nodes as part of query plan termination. It follows the standard PostgreSQL executor cleanup pattern by first allowing child nodes to clean up their resources through ExecEndNode, then performing Gather-specific cleanup through ExecShutdownGather. The ExecShutdownGather function handles the complex process of shutting down parallel workers and destroying the parallel context, ensuring that all worker processes are properly terminated and shared memory resources are released.

This function is critical for resource management in parallel queries, as it ensures that worker processes don't become orphaned and that shared memory segments used for inter-process communication are properly cleaned up when queries complete or are cancelled.

## Parameters / Member Variables
- : The GatherState containing all parallel execution state and worker information to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [ExecEndNode](ExecEndNode.md) (cleans up the outer child plan node recursively)
  - outerPlanState (accesses the child plan state)
  - [ExecShutdownGather](ExecShutdownGather.md) (performs Gather-specific parallel worker shutdown)
    - [ExecShutdownGatherWorkers](ExecShutdownGatherWorkers.md) (terminates individual worker processes)
    - [ExecParallelCleanup](ExecParallelCleanup.md) (destroys parallel execution context and shared memory)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (main node cleanup dispatcher)

## Notes and Other Information
- Follows the cleanup ordering principle: child nodes are cleaned up before parent nodes
- The actual parallel worker termination is handled by ExecShutdownGather, which manages worker process lifecycle
- Essential for preventing resource leaks in parallel queries
- Called during both normal query completion and error/cancellation scenarios
- The cleanup is idempotent - can be safely called multiple times
- Works in conjunction with PostgreSQL's resource management system to ensure clean shutdown

## Simplified Source

```c
void ExecEndGather(GatherState *node)
{
    // Clean up child plan first
    ExecEndNode(outerPlanState(node));

    // Shut down parallel workers and release parallel context
    ExecShutdownGather(node);
}
```