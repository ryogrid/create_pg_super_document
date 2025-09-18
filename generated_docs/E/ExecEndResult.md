# ExecEndResult

## Location
[src/backend/executor/nodeResult.c:240-248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeResult.c#L240-L248)

## Overview
ExecEndResult performs cleanup and resource deallocation for a Result plan node by shutting down any child nodes.

## Definition


## Detailed Description
ExecEndResult is the cleanup function for Result plan nodes that is called during executor shutdown. This function is part of PostgreSQL's executor termination phase and ensures proper resource cleanup.

The function performs minimal cleanup specific to Result nodes:
- **Child node shutdown**: If the Result node has an outer plan (child node), it recursively calls ExecEndNode to properly shut down the child and free its resources
- **Automatic cleanup**: The PostgreSQL memory context system automatically handles most memory deallocation for the Result node's own structures (ResultState, expressions, etc.)

Result nodes are relatively lightweight and don't maintain complex state that requires explicit cleanup beyond ensuring their child nodes are properly terminated. The bulk of resource management is handled by the memory context framework.

## Parameters / Member Variables
- : The ResultState to be cleaned up and shut down

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState (to get the outer plan state for cleanup)
  - [ExecEndNode](ExecEndNode.md) (to recursively shut down child nodes)
- Called from:
  - [ExecEndNode](ExecEndNode.md) (the main node termination dispatcher in execProcnode.c)
  - Declared in nodeResult.h

## Notes and Other Information
- The function only needs to handle outer plan cleanup since Result nodes never have inner (right) child plans
- Memory allocated for the ResultState structure and associated expressions is automatically freed by the memory context system
- This is part of PostgreSQL's hierarchical cleanup pattern where parent nodes are responsible for cleaning up their children
- The function's simplicity reflects that Result nodes don't maintain persistent resources beyond their child nodes
- Called during query termination, transaction abort, or when freeing executor state