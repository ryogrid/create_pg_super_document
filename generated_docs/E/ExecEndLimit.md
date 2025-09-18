# ExecEndLimit

## Location
[src/backend/executor/nodeLimit.c:534-540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeLimit.c#L534-L540)

## Overview
ExecEndLimit performs cleanup for a Limit node by shutting down its subplan and freeing associated resources.

## Definition


## Detailed Description
ExecEndLimit is the cleanup function for Limit execution nodes, responsible for properly terminating execution and releasing resources when a Limit node is no longer needed. The function follows the standard PostgreSQL execution node cleanup pattern by recursively calling ExecEndNode on its child plan.

The function is minimal because Limit nodes have minimal resource requirements beyond their child plan. The PostgreSQL memory management system handles automatic cleanup of the LimitState structure and associated memory contexts, so explicit resource deallocation is generally not required.

## Parameters / Member Variables
- : LimitState containing the execution state to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [ExecEndNode](ExecEndNode.md) (recursively shuts down child plan)
  - outerPlanState (accesses child plan state)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (as part of plan tree cleanup)

## Notes and Other Information
- Follows PostgreSQL's recursive cleanup pattern for plan nodes
- Memory management is largely automatic through memory context cleanup
- No explicit cleanup of tuple slots or expression contexts is needed as they are managed by the memory context system
- The function ensures that child plans are properly terminated before the parent Limit node is destroyed
- This function is called during query completion or when execution is aborted