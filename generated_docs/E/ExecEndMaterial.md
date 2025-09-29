# ExecEndMaterial

## Location
[src/backend/executor/nodeMaterial.c:240-261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMaterial.c#L240-L261)

## Overview
ExecEndMaterial cleans up and releases resources used by a MaterialState node, including the tuplestore and child plan nodes.

## Definition

```c
void
ExecEndMaterial(MaterialState *node)
```
## Detailed Description
ExecEndMaterial performs cleanup operations for a Material node when execution is complete. It releases the tuplestore resources if they were allocated and properly shuts down the child subplan node. This function ensures that all memory and resources associated with the Material node are properly freed to prevent resource leaks.

The function is straightforward in its operation: it first checks if a tuplestore was created and releases it if present, then recursively shuts down the child plan node.

## Parameters / Member Variables
- : The MaterialState node to be cleaned up and its resources released

## Dependencies
- Functions called/Symbols referenced:
  - [MaterialState](../M/MaterialState.md)
  - [tuplestore_end](../t/tuplestore_end.md)
  - [ExecEndNode](ExecEndNode.md)
  - outerPlanState
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md)

## Notes and Other Information
- The function is defensive - it checks if tuplestorestate is NULL before attempting to release it
- After releasing the tuplestore, it sets the pointer to NULL to prevent double-free errors
- The cleanup order is important: tuplestore resources are released before shutting down child nodes
- This function is part of the standard PostgreSQL executor cleanup pattern

## Simplified Source

```c
void ExecEndMaterial(MaterialState *node) {
    // Release tuplestore resources if they exist
    if (node->tuplestorestate != NULL)
        tuplestore_end(node->tuplestorestate);
    node->tuplestorestate = NULL;

    // Recursively shut down the child plan
    ExecEndNode(outerPlanState(node));
}
```