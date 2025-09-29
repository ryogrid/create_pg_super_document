# ExecShutdownNode

## Location
[src/backend/executor/execProcnode.c:767-772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execProcnode.c#L767-L772)

## Overview
ExecShutdownNode provides a controlled shutdown mechanism for query plan execution trees, allowing nodes to stop asynchronous operations and release resources before complete termination.

## Definition
```c
void ExecShutdownNode(PlanState *node)
```

## Detailed Description
ExecShutdownNode serves as a high-level interface for graceful shutdown of PostgreSQL execution plan trees. Unlike ExecEndNode which performs complete cleanup and makes nodes unusable, ExecShutdownNode focuses on stopping active asynchronous operations and releasing resources while potentially keeping the node structure intact.

The function acts as a simple wrapper that delegates the actual shutdown logic to ExecShutdownNode_walker, which implements a tree walking pattern to recursively shutdown all nodes in the plan tree. This separation allows for a clean public interface while implementing the complex traversal and node-specific shutdown logic internally.

The shutdown process is particularly important for nodes that manage:
- Parallel workers (Gather, GatherMerge nodes)
- Foreign table connections (ForeignScan nodes)  
- Hash tables and temporary structures
- Custom scan operations that may have ongoing background processes

This function is typically called during query execution interruption or when stopping execution before completion, providing a cleaner alternative to immediate termination.

## Parameters / Member Variables
- `node`: PlanState pointer to the root of the plan tree to be shut down; can be NULL for safe operation

## Dependencies
- Functions called/Symbols referenced:
  - [ExecShutdownNode_walker](ExecShutdownNode_walker.md) (performs the actual tree traversal and shutdown operations)
- Called from (representative examples):
  - [ExecutePlan](ExecutePlan.md) (during query execution control)
  - EvalPlanQualSetSlot (EPQ operation management)

## Notes and Other Information
- Designed for graceful shutdown rather than complete cleanup (use ExecEndNode for final cleanup)
- Particularly important for parallel query execution where worker processes need controlled shutdown
- Safe to call on partially executed plans or plans that have never executed
- Works in conjunction with PostgreSQL's instrumentation system to properly track resource usage during shutdown
- The walker pattern implementation allows for extensible shutdown behavior for different node types
- Less comprehensive than ExecEndNode but provides essential resource management for active operations

## Simplified Source

```c
void
ExecShutdownNode(PlanState *node)
{
    // Delegate to tree walker to recursively shutdown all nodes
    (void) ExecShutdownNode_walker(node, NULL);
}
```