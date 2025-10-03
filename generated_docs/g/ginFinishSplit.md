# ginFinishSplit

## Location
[src/backend/access/gin/ginbtree.c:672-778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginbtree.c#L672-L778)

## Overview
ginFinishSplit completes a B-tree page split by inserting the downlink for the newly created page into the parent, recursively handling splits up the tree until the operation is complete.

## Definition
```c
static void ginFinishSplit(GinBtree btree, GinBtreeStack *stack, bool freestack,
                          GinStatsData *buildStats)
```

## Detailed Description
ginFinishSplit handles the completion phase of GIN B-tree splits by inserting downlinks into parent pages. The function operates through a loop that crawls up the tree stack, inserting downlinks at each level until a parent page has enough space to accommodate the new downlink without splitting.

Key operational aspects:
1. **Parent Location**: Uses the btree's findChildPtr method to locate the correct insertion point in the parent
2. **Incomplete Split Handling**: Detects and completes any incomplete splits encountered in parent pages via ginFinishOldSplit
3. **Right Movement**: Navigates rightward across sibling pages when the target child pointer is not found on the current parent page
4. **Recursive Completion**: Continues up the tree until ginPlaceToPage returns true, indicating no further splits are needed

The function handles buffer management based on the freestack parameter - either releasing all buffers as it traverses upward or maintaining locks for caller use.

## Parameters / Member Variables
- `btree`: GinBtree structure containing method pointers and metadata
- `stack`: GinBtreeStack representing the path from root to the page that was split
- `freestack`: Boolean controlling whether to release buffers and free stack during traversal
- `buildStats`: Statistics structure for tracking index build progress

## Dependencies
- Functions called/Symbols referenced:
  - [LockBuffer](../L/LockBuffer.md), UnlockReleaseBuffer, BufferGetPage
  - GinPageIsIncompleteSplit, GinPageRightMost, GinPageGetOpaque
  - [ginFinishOldSplit](ginFinishOldSplit.md), ginFindParents, ginStepRight
  - [ginPlaceToPage](ginPlaceToPage.md), freeGinBtreeStack
  - btree->findChildPtr, btree->prepareDownlink
- Called from:
  - [ginFinishOldSplit](ginFinishOldSplit.md) (src/backend/access/gin/ginbtree.c:800)
  - [ginInsertValue](ginInsertValue.md) (src/backend/access/gin/ginbtree.c:834)

## Notes and Other Information
The function includes injection points for testing incomplete split scenarios. It handles the complex case where parent pages themselves may be incompletely split, ensuring all incomplete splits are resolved before proceeding. The right-movement logic handles the case where concurrent splits may have moved the target child to a different parent page. When freestack is false, only the bottom page remains locked for caller use.

## Simplified Source

```c
static void
ginFinishSplit(GinBtree btree, GinBtreeStack *stack, bool freestack,
               GinStatsData *buildStats)
{
    Page page;
    bool done;
    bool first = true;

    // Loop up the tree until split completion
    do
    {
        GinBtreeStack *parent = stack->parent;
        void *insertdata;
        BlockNumber updateblkno;

        // Lock parent page
        LockBuffer(parent->buffer, GIN_EXCLUSIVE);

        // Complete any incomplete splits in parent
        if (GinPageIsIncompleteSplit(BufferGetPage(parent->buffer)))
            ginFinishOldSplit(btree, parent, buildStats, GIN_EXCLUSIVE);

        // Find correct parent position, moving right if needed
        page = BufferGetPage(parent->buffer);
        while ((parent->off = btree->findChildPtr(btree, page, stack->blkno, parent->off)) == InvalidOffsetNumber)
        {
            if (GinPageRightMost(page))
            {
                // Need to search from root to find parent
                LockBuffer(parent->buffer, GIN_UNLOCK);
                ginFindParents(btree, stack);
                parent = stack->parent;
                break;
            }

            parent->buffer = ginStepRight(parent->buffer, btree->index, GIN_EXCLUSIVE);
            parent->blkno = BufferGetBlockNumber(parent->buffer);
            page = BufferGetPage(parent->buffer);

            if (GinPageIsIncompleteSplit(BufferGetPage(parent->buffer)))
                ginFinishOldSplit(btree, parent, buildStats, GIN_EXCLUSIVE);
        }

        // Prepare downlink data and insert to parent
        insertdata = btree->prepareDownlink(btree, stack->buffer);
        updateblkno = GinPageGetOpaque(BufferGetPage(stack->buffer))->rightlink;
        done = ginPlaceToPage(btree, parent,
                             insertdata, updateblkno,
                             stack->buffer, buildStats);
        pfree(insertdata);

        // Release child buffer if requested or not first iteration
        if (!first || freestack)
            LockBuffer(stack->buffer, GIN_UNLOCK);
        if (freestack)
        {
            ReleaseBuffer(stack->buffer);
            pfree(stack);
        }
        stack = parent;

        first = false;
    } while (!done);

    // Unlock final parent
    LockBuffer(stack->buffer, GIN_UNLOCK);

    if (freestack)
        freeGinBtreeStack(stack);
}
```