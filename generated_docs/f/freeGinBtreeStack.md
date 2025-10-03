# freeGinBtreeStack

## Location
[src/backend/access/gin/ginbtree.c:198-217](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginbtree.c#L198-L217)

## Overview
freeGinBtreeStack deallocates a GinBtreeStack structure and all its parent nodes, properly releasing associated buffers to prevent memory and buffer leaks.

## Definition
void freeGinBtreeStack(GinBtreeStack *stack)

## Detailed Description
freeGinBtreeStack is a cleanup utility function that traverses and deallocates a linked list of GinBtreeStack nodes from leaf to root. The function follows the parent pointer chain, releasing each buffer (if valid) and freeing the associated memory for each stack node. This ensures proper resource cleanup after GIN B-tree operations complete, preventing both memory leaks and buffer reference leaks that could lead to buffer pool exhaustion.

## Parameters / Member Variables
- `stack`: Pointer to the GinBtreeStack structure to be freed (typically the leaf node of a stack chain)

## Dependencies
- Functions called/Symbols referenced:
  - [ReleaseBuffer](../R/ReleaseBuffer.md) (buffer deallocation)
  - [pfree](../p/pfree.md) (memory deallocation)
  - InvalidBuffer (buffer validity check constant)
- Called from (representative examples):
  - [ginFinishSplit](../g/ginFinishSplit.md)
  - [ginInsertValue](../g/ginInsertValue.md)
  - [scanPostingTree](../s/scanPostingTree.md)
  - [startScanEntry](../s/startScanEntry.md)
  - [entryLoadMoreItems](../e/entryLoadMoreItems.md)
  - [ginEntryInsert](../g/ginEntryInsert.md)

## Notes and Other Information
The function implements a safe traversal pattern that handles the stack destruction without corrupting the linked list structure. It checks for InvalidBuffer before attempting to release buffers, ensuring robustness when dealing with partially constructed stacks or error conditions. This function is essential for preventing resource leaks in GIN operations and is called whenever a GinBtreeStack is no longer needed, regardless of whether the operation completed successfully or encountered an error.

## Simplified Source

```c
void
freeGinBtreeStack(GinBtreeStack *stack)
{
    // Traverse stack from leaf to root, freeing resources
    while (stack)
    {
        GinBtreeStack *tmp = stack->parent;

        // Release buffer if valid
        if (stack->buffer != InvalidBuffer)
            ReleaseBuffer(stack->buffer);

        // Free current stack node
        pfree(stack);
        stack = tmp;
    }
}
```