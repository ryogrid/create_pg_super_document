# ExecEndGroup

## Location
[src/backend/executor/nodeGroup.c:226-234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGroup.c#L226-L234)

## Overview
ExecEndGroup performs cleanup and resource deallocation for a Group plan node during query execution shutdown, primarily by recursively ending its child plan node.

## Definition


## Detailed Description
ExecEndGroup is responsible for the orderly shutdown of a Group plan node during query completion or early termination. The function follows PostgreSQL's standard cleanup protocol by recursively calling ExecEndNode on its child plan node to ensure proper resource deallocation throughout the plan tree.

The cleanup process is straightforward but essential for preventing resource leaks:
1. Obtains reference to the outer (child) plan state
2. Calls ExecEndNode to recursively clean up the child subtree
3. Allows the parent cleanup process to handle GroupState-specific deallocation

This function is part of PostgreSQL's disciplined resource management system where each node type is responsible for cleaning up its own resources and ensuring its children are properly shut down.

## Parameters / Member Variables
- : The GroupState to be cleaned up and shut down

## Dependencies
- Functions called/Symbols referenced:
  - [GroupState](../G/GroupState.md) (node parameter type)
  - [PlanState](../P/PlanState.md) (outer plan reference)
  - outerPlanState (child plan access macro)
  - [ExecEndNode](ExecEndNode.md) (recursive cleanup)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (during plan tree cleanup)

## Notes and Other Information
- Follows PostgreSQL's standard bottom-up cleanup pattern
- Does not explicitly free GroupState memory as this is handled by memory context cleanup
- Critical for preventing resource leaks in complex query plans
- The outer plan cleanup handles all child node resources including tuple slots and expression contexts
- Part of the three-phase node lifecycle: Init -> Exec -> End