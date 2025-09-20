# ExecReScanResult

## Location
[src/backend/executor/nodeResult.c:249-262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeResult.c#L249-L262)

## Overview
Reinitializes a Result node to start scanning from the beginning, resetting internal state flags and conditionally rescanning the outer (child) plan node.

## Definition

```c
void
ExecReScanResult(ResultState *node)
```
## Detailed Description
ExecReScanResult is part of PostgreSQL's executor framework responsible for reinitializing Result plan nodes during query execution. Result nodes are simple plan nodes that typically apply constant qualifications or perform one-time operations. This function resets the node's internal state to prepare for a fresh scan of the data.

The function performs three key operations:
1. Resets the  flag to false, indicating the node is ready to produce results again
2. Reinitializes the  flag based on whether a constant qualification expression exists
3. Conditionally rescans the outer (child) plan node, but only if the child node's parameters haven't changed (chgParam is NULL)

The conditional rescanning logic is an optimization: if the child node's parameters have changed, it will be automatically rescanned on the next call to ExecProcNode, so an explicit rescan here would be redundant.

## Parameters / Member Variables
- : Pointer to the ResultState structure containing the state information for this Result plan node
  - : Boolean flag indicating whether the node has finished producing results
  - : Boolean flag indicating whether constant qualifications need to be checked
  - : Expression state for constant qualification conditions

## Dependencies
- Functions called/Symbols referenced:
  - : Macro/function to access the outer (child) plan state
  - : Generic rescan function for plan nodes
- Called from (representative examples):
  -  (src/backend/executor/execAmi.c:133): Generic executor rescan dispatcher

## Notes and Other Information
- This function is part of the standard executor node interface, implementing the rescan operation for Result nodes
- The function is declared in src/include/executor/nodeResult.h and follows the naming convention ExecReScan[NodeType]
- [Result](../R/Result.md) nodes are often used for constant expressions, one-time filters, or as leaf nodes in execution trees
- The optimization to avoid unnecessary rescanning when chgParam is non-NULL is a common pattern across executor node types
- Located in src/backend/executor/nodeResult.c:249-262