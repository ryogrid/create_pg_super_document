# ExecEndUnique

## Location
[src/backend/executor/nodeUnique.c:168-174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeUnique.c#L168-L174)

## Overview
ExecEndUnique performs cleanup and resource deallocation for a UNIQUE plan node by shutting down its outer subplan and freeing associated resources.

## Definition
void ExecEndUnique(UniqueState *node)

## Detailed Description
ExecEndUnique is responsible for the orderly shutdown of a UNIQUE plan node during query execution cleanup. The function performs a simple but essential task: it recursively calls ExecEndNode on the outer subplan to ensure that all child nodes in the execution tree are properly shut down and their resources are freed. This follows the standard PostgreSQL executor pattern where each node is responsible for cleaning up its immediate children. The function itself does not need to perform additional cleanup beyond shutting down the subplan, as the UniqueState structure and other associated resources are managed by the memory context system and will be automatically freed when the query context is destroyed.

## Parameters / Member Variables
- : Pointer to the UniqueState structure representing the UNIQUE node to be shut down

## Dependencies
- Functions called/Symbols referenced:
  - [ExecEndNode](ExecEndNode.md): Recursively shut down the outer subplan node
  - outerPlanState: Access the outer plan state from the UniqueState structure
- Called from:
  - [ExecEndNode](ExecEndNode.md): During query execution cleanup
  - nodeUnique.h: Header declaration

## Notes and Other Information
- Follows the standard executor cleanup pattern of recursively shutting down child nodes
- Does not require explicit memory deallocation as resources are managed by PostgreSQL's memory context system
- Simple implementation reflects the stateless nature of the UNIQUE node operation
- Part of the standard executor node lifecycle: Init -> Execute -> End

## Simplified Source

```c
void ExecEndUnique(UniqueState *node) {
    // Shut down the outer subplan
    ExecEndNode(outerPlanState(node));
}
```