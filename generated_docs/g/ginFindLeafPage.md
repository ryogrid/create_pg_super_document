# ginFindLeafPage

## Location
[src/backend/access/gin/ginbtree.c:83-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginbtree.c#L83-L176)

## Overview
ginFindLeafPage descends the GIN B-tree to locate the leaf page that contains or would contain a specified key, handling path maintenance and locking appropriately based on the operation type.

## Definition
GinBtreeStack *ginFindLeafPage(GinBtree btree, bool searchMode, bool rootConflictCheck)

## Detailed Description
ginFindLeafPage is a fundamental tree traversal function in PostgreSQL's GIN index implementation that navigates from the root to the appropriate leaf page. The function maintains a stack representing the path from root to leaf, with different locking strategies based on the operation type. In search mode, it maintains only shared locks and doesn't preserve the full path for memory efficiency. In modification mode, it maintains exclusive locks and preserves the complete path stack for potential splits. The function handles incomplete page splits encountered during traversal and supports both targeted searches and full scans.

## Parameters / Member Variables
- : GinBtree structure containing index metadata, search key, and operation-specific callbacks
- : Boolean flag - true for read-only operations, false for modification operations requiring exclusive locks
- : Boolean flag to enable serialization conflict checking at the tree root

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [ReadBuffer](../R/ReadBuffer.md), ReleaseAndReadBuffer (buffer management)
  - [CheckForSerializableConflictIn](../C/CheckForSerializableConflictIn.md) (serialization conflict detection)
  - [ginTraverseLock](ginTraverseLock.md) (buffer locking)
  - GinPageIsIncompleteSplit, ginFinishOldSplit (split handling)
  - GinPageGetOpaque, GinPageIsLeaf (page inspection)
  - [ginStepRight](ginStepRight.md) (rightward page navigation)
  - [LockBuffer](../L/LockBuffer.md) (buffer locking operations)
- Called from (representative examples):
  - [ginInsertItemPointers](ginInsertItemPointers.md)
  - [ginScanBeginPostingTree](ginScanBeginPostingTree.md)
  - [startScanEntry](../s/startScanEntry.md)
  - [entryLoadMoreItems](../e/entryLoadMoreItems.md)
  - [ginEntryInsert](ginEntryInsert.md)

## Notes and Other Information
The function implements an optimized traversal strategy where search operations don't maintain the full path stack to reduce memory overhead. The right-link following logic handles the case where concurrent operations may cause the search to land on the wrong page initially. The incomplete split handling ensures index consistency even in the presence of interrupted operations. The predictNumber field in the stack is used for buffer prefetching optimization during tree traversal.

## Simplified Source

```c
GinBtreeStack *
ginFindLeafPage(GinBtree btree, bool searchMode, bool rootConflictCheck)
{
    // Initialize stack starting from root
    GinBtreeStack *stack = (GinBtreeStack *) palloc(sizeof(GinBtreeStack));
    stack->blkno = btree->rootBlkno;
    stack->buffer = ReadBuffer(btree->index, btree->rootBlkno);
    stack->parent = NULL;
    stack->predictNumber = 1;

    // Check for serialization conflicts if requested
    if (rootConflictCheck)
        CheckForSerializableConflictIn(btree->index, NULL, btree->rootBlkno);

    // Descend tree until we reach a leaf page
    for (;;)
    {
        Page page;
        BlockNumber child;

        stack->off = InvalidOffsetNumber;
        page = BufferGetPage(stack->buffer);

        // Acquire appropriate lock for operation type
        int access = ginTraverseLock(stack->buffer, searchMode);

        // Complete any incomplete splits encountered
        if (!searchMode && GinPageIsIncompleteSplit(page))
            ginFinishOldSplit(btree, stack, NULL, access);

        // Move right if needed to find correct page
        while (btree->fullScan == false && stack->blkno != btree->rootBlkno &&
               btree->isMoveRight(btree, page))
        {
            BlockNumber rightlink = GinPageGetOpaque(page)->rightlink;
            if (rightlink == InvalidBlockNumber)
                break;

            stack->buffer = ginStepRight(stack->buffer, btree->index, access);
            stack->blkno = rightlink;
            page = BufferGetPage(stack->buffer);

            if (!searchMode && GinPageIsIncompleteSplit(page))
                ginFinishOldSplit(btree, stack, NULL, access);
        }

        // Return if we found a leaf page
        if (GinPageIsLeaf(page))
            return stack;

        // Find child page to descend to
        child = btree->findChildPage(btree, stack);
        LockBuffer(stack->buffer, GIN_UNLOCK);

        if (searchMode)
        {
            // In search mode, don't maintain full path
            stack->blkno = child;
            stack->buffer = ReleaseAndReadBuffer(stack->buffer, btree->index, stack->blkno);
        }
        else
        {
            // In modification mode, maintain full path stack
            GinBtreeStack *ptr = (GinBtreeStack *) palloc(sizeof(GinBtreeStack));
            ptr->parent = stack;
            stack = ptr;
            stack->blkno = child;
            stack->buffer = ReadBuffer(btree->index, stack->blkno);
            stack->predictNumber = 1;
        }
    }
}
```