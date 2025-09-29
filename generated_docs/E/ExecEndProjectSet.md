# ExecEndProjectSet

## Location
[src/backend/executor/nodeProjectSet.c:328-336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeProjectSet.c#L328-L336)

## Overview
ExecEndProjectSet performs cleanup and resource deallocation for ProjectSet nodes when execution completes.

## Definition
```c
void ExecEndProjectSet(ProjectSetState *node)
```

## Detailed Description
ExecEndProjectSet is responsible for properly shutting down and cleaning up resources associated with a ProjectSet node. The function focuses primarily on ensuring that child plan nodes are properly terminated through the executor node hierarchy.

The cleanup is relatively straightforward compared to other node types because:
1. ProjectSet nodes only have outer child plans (no inner plans)
2. Most memory management is handled automatically through PostgreSQL's memory context system
3. The specialized argcontext created during initialization is automatically freed when its parent context is destroyed

## Parameters / Member Variables
- `node`: The ProjectSetState to be cleaned up and deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [ExecEndNode](ExecEndNode.md)
  - outerPlanState
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (as part of plan tree cleanup)

## Notes and Other Information
- This is one of the simpler cleanup functions in the executor
- The function relies on PostgreSQL's memory context management for most cleanup
- Only explicitly calls ExecEndNode on the outer child plan
- Part of the standard executor node lifecycle (Init -> Exec -> End)

## Simplified Source
```c
void ExecEndProjectSet(ProjectSetState *node) {
    // Shut down subplans - cleanup outer child plan
    ExecEndNode(outerPlanState(node));
}
```