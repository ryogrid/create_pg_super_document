# ExecEndSubqueryScan

## Location
src/backend/executor/nodeSubqueryscan.c: 168 - 182

## Overview
ExecEndSubqueryScan performs cleanup operations for a SubqueryScan node, freeing any allocated storage and properly shutting down the subquery execution.

## Definition
```c
void ExecEndSubqueryScan(SubqueryScanState *node)
```

## Detailed Description
ExecEndSubqueryScan is the cleanup function for subquery scan operations in PostgreSQL's executor. It follows the standard executor cleanup pattern by recursively calling ExecEndNode on the underlying subplan to ensure proper resource deallocation. The function is deliberately simple, as most of the SubqueryScan node's resources are managed automatically through the memory context system, and the primary responsibility is to ensure that the subplan is properly shut down. This design reflects PostgreSQL's approach to resource management where each node type is responsible for cleaning up its own resources and delegating cleanup of child nodes to the appropriate functions.

## Parameters / Member Variables
- `node`: A SubqueryScanState pointer containing the execution state that needs to be cleaned up, including the subplan that must be shut down

## Dependencies
- Functions called/Symbols referenced:
  - ExecEndNode (recursively shuts down the subplan)
  - SubqueryScanState (execution state structure)
- Called from (representative examples):
  - ExecEndNode (part of the general executor node cleanup framework)

## Notes and Other Information
- The function has no return value as cleanup operations are expected to complete successfully
- Follows PostgreSQL's standard executor cleanup pattern with recursive delegation to child nodes
- Most memory cleanup is handled automatically through memory context destruction
- Primary responsibility is ensuring proper shutdown of the underlying subplan execution
- Part of PostgreSQL's executor cleanup framework that ensures proper resource deallocation across all node types
- Located at src/backend/executor/nodeSubqueryscan.c:168-182