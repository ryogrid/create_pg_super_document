# ginFindParents

## Location
[src/backend/access/gin/ginbtree.c:218-336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginbtree.c#L218-L336)

## Overview
ginFindParents reconstructs the parent path for a GIN B-tree stack by traversing from the root down to locate the parent of a specified child page.

## Definition
static void ginFindParents(GinBtree btree, GinBtreeStack *stack)

## Detailed Description
ginFindParents is a complex tree navigation function that rebuilds the parent-child relationship in a GIN B-tree stack when the path has been lost or corrupted. The function starts by unwinding the current stack to the root while carefully maintaining the root buffer pin to prevent concurrent VACUUM operations. It then performs a top-down traversal from the root, searching for the parent of the target child page (stack->blkno). The function handles incomplete page splits encountered during traversal and uses right-link following when the target child is not found on the current page. This operation is essential for maintaining tree structure integrity during complex operations like page splits.

## Parameters / Member Variables
- `btree`: GinBtree structure containing index metadata and tree operation callbacks
- `stack`: GinBtreeStack representing the child page whose parent needs to be found

## Dependencies
- Functions called/Symbols referenced:
  - [ReleaseBuffer](../R/ReleaseBuffer.md) (buffer deallocation)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (buffer metadata access)
  - [palloc](../p/palloc.md) (memory allocation)
  - [LockBuffer](../L/LockBuffer.md) (buffer locking operations)
  - [BufferGetPage](../B/BufferGetPage.md) (page retrieval from buffer)
  - GinPageIsLeaf, GinPageIsIncompleteSplit (page type checking)
  - [ginFinishOldSplit](ginFinishOldSplit.md) (incomplete split handling)
  - GinPageGetOpaque (page metadata access)
  - [ginStepRight](ginStepRight.md) (rightward page navigation)
  - [ReadBuffer](../R/ReadBuffer.md) (buffer allocation and reading)
  - elog (error logging)
- Called from (representative examples):
  - [ginFinishSplit](ginFinishSplit.md)

## Notes and Other Information
The function implements a sophisticated algorithm that maintains the critical root buffer pin throughout the operation, preventing concurrent VACUUM from interfering with the tree structure. The parent reconstruction may require multiple iterations due to concurrent page splits, with ginFinishOldSplit recursively calling ginFindParents if needed. The function carefully handles edge cases like reaching the rightmost page without finding the target child, indicating the child has moved to a different level. This function is primarily used during page split operations when the original parent path becomes invalid due to structural changes in the tree.

## Simplified Source

```c
static void
ginFindParents(GinBtree btree, GinBtreeStack *stack)
{
    Page page;
    Buffer buffer;
    BlockNumber blkno, leftmostBlkno;
    OffsetNumber offset;
    GinBtreeStack *root;
    GinBtreeStack *ptr;

    // Unwind stack to root, keeping root pin to prevent VACUUM conflicts
    root = stack->parent;
    while (root->parent)
    {
        ReleaseBuffer(root->buffer);
        root = root->parent;
    }

    // Start from root page
    blkno = root->blkno;
    buffer = root->buffer;
    ptr = (GinBtreeStack *) palloc(sizeof(GinBtreeStack));

    // Descend tree to find parent of target child
    for (;;)
    {
        LockBuffer(buffer, GIN_EXCLUSIVE);
        page = BufferGetPage(buffer);

        // Complete any incomplete splits
        if (GinPageIsIncompleteSplit(page))
        {
            ptr->blkno = blkno;
            ptr->buffer = buffer;
            ptr->parent = root;
            ptr->off = InvalidOffsetNumber;
            ginFinishOldSplit(btree, ptr, NULL, GIN_EXCLUSIVE);
        }

        leftmostBlkno = btree->getLeftMostChild(btree, page);

        // Search for child pointer on current level
        while ((offset = btree->findChildPtr(btree, page, stack->blkno, InvalidOffsetNumber)) == InvalidOffsetNumber)
        {
            // Move right to continue search
            blkno = GinPageGetOpaque(page)->rightlink;
            if (blkno == InvalidBlockNumber)
            {
                // Child not found at this level
                LockBuffer(buffer, GIN_UNLOCK);
                if (buffer != root->buffer)
                    ReleaseBuffer(buffer);
                break;
            }

            buffer = ginStepRight(buffer, btree->index, GIN_EXCLUSIVE);
            page = BufferGetPage(buffer);

            if (GinPageIsIncompleteSplit(page))
            {
                ptr->blkno = blkno;
                ptr->buffer = buffer;
                ptr->parent = root;
                ptr->off = InvalidOffsetNumber;
                ginFinishOldSplit(btree, ptr, NULL, GIN_EXCLUSIVE);
            }
        }

        // Found parent or need to descend further
        if (blkno != InvalidBlockNumber)
        {
            ptr->blkno = blkno;
            ptr->buffer = buffer;
            ptr->parent = root;
            ptr->off = offset;
            stack->parent = ptr;
            return;
        }

        // Descend to next level
        blkno = leftmostBlkno;
        buffer = ReadBuffer(btree->index, blkno);
    }
}
```