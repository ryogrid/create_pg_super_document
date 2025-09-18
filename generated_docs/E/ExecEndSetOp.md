# ExecEndSetOp

## Location
src/backend/executor/nodeSetOp.c: 583 - 593

## Overview
ExecEndSetOp is a cleanup function for SetOp execution nodes that shuts down the subplan and frees resources allocated to the node during set operation processing.

## Definition
```c
void ExecEndSetOp(SetOpState *node)
```

## Detailed Description
ExecEndSetOp performs the cleanup operations required when a SetOp execution node completes processing. This function is responsible for properly shutting down the node and releasing all resources that were allocated during the node's lifetime. The function handles two main cleanup tasks: freeing the hashtable memory context if it exists, and recursively ending the outer subplan node. This ensures that all memory allocated for set operations (such as UNION, INTERSECT, EXCEPT) is properly released and prevents memory leaks in the executor.

## Parameters / Member Variables
- `node`: A pointer to the SetOpState structure representing the set operation execution state that needs to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (conditionally frees the hashtable memory context)
  - [ExecEndNode](ExecEndNode.md) (recursively ends the outer subplan)
  - outerPlanState (macro to access the outer plan state)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (from src/backend/executor/execProcnode.c:737)

## Notes and Other Information
- This function is part of the PostgreSQL executor's node cleanup framework
- The tableContext memory context is only deleted if it exists (non-NULL check)
- The function follows the standard PostgreSQL executor pattern of recursively cleaning up child nodes
- Declared in src/include/executor/nodeSetOp.h and defined in src/backend/executor/nodeSetOp.c:583-593