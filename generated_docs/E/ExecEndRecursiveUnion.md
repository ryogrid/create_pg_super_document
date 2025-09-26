# ExecEndRecursiveUnion

## Location
src/backend/executor/nodeRecursiveunion.c: 272 - 297

## Overview
Performs cleanup and resource deallocation for a RecursiveUnion plan node, releasing all memory and storage allocated during recursive query execution.

## Definition

```c
void
ExecEndRecursiveUnion(RecursiveUnionState *node)
```
## Detailed Description
The `ExecEndRecursiveUnion` function is responsible for the orderly cleanup of all resources allocated for recursive UNION query execution. This function is called when the RecursiveUnion node is being shut down, either at the end of query execution or when the node is being destroyed.

The cleanup process follows a systematic approach:
1. **Tuple Store Cleanup**: Releases both the working table and intermediate table tuple stores that were used for managing recursive iterations
2. **Memory Context Cleanup**: Deletes the specialized memory contexts created for hash table operations (tempContext) and hash table storage (tableContext), if they were allocated
3. **Child Node Cleanup**: Recursively calls `ExecEndNode` on both the outer (non-recursive) and inner (recursive) child plan nodes to ensure proper cleanup of the entire plan subtree

This function ensures that no memory leaks occur and that all resources are properly returned to the system, maintaining PostgreSQL's memory management discipline.

## Parameters / Member Variables
- `node`: Pointer to the RecursiveUnionState structure containing all the execution state and resources to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - tuplestore_end
  - MemoryContextDelete
  - ExecEndNode
  - outerPlanState
  - innerPlanState
- Called from (representative examples):
  - ExecEndNode

## Notes and Other Information
- Part of PostgreSQL's systematic resource cleanup framework for plan nodes
- Safely handles cases where memory contexts may be NULL (when no duplicate elimination was used)
- Ensures complete cleanup of the plan node subtree by recursively ending child nodes
- Critical for preventing memory leaks in long-running queries or repeated recursive operations
- Follows PostgreSQL's memory management best practices for executor nodes
- Must be called to properly terminate recursive UNION query execution