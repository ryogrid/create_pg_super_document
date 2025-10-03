# gistFindCorrectParent

## Location
[src/backend/access/gist/gist.c:1022-1134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gist.c#L1022-L1134)

## Overview
Updates the insertion stack to ensure that child->parent points to the correct parent page, handling cases where the parent has been modified due to concurrent operations or page splits.

## Definition

```c
static void
gistFindCorrectParent(Relation r, GISTInsertStack *child, bool is_build)
```
## Detailed Description
 is a critical function for maintaining parent-child relationships in the GiST tree when concurrent modifications may have invalidated the cached parent pointer in the insertion stack. The function handles several scenarios:

1. **Quick Verification**: First checks if the downlink is still at the expected offset in the parent page
2. **Local Search**: If the downlink moved within the same page, scans the entire page to find it
3. **Right Link Following**: If the page was split, follows right links to locate the downlink on sibling pages
4. **Full Tree Search**: In rare cases (like root splits), performs a complete tree search using 
5. **Recursive Correction**: After finding the new parent chain, recursively calls itself to ensure the entire path is correct

The function must handle different scenarios during normal operations versus index builds, where WAL logging behavior differs.

## Parameters / Member Variables
- : The GiST index relation
- : Pointer to the insertion stack entry for the child page whose parent needs to be found/corrected
- : Boolean indicating whether this is called during index build (affects LSN checking behavior)

## Dependencies
- Functions called/Symbols referenced:
  - [gistcheckpage](gistcheckpage.md)
  - [gistFindPath](gistFindPath.md)
  - GistPageGetOpaque
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetLSN](../P/PageGetLSN.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ReadBuffer](../R/ReadBuffer.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
- Called from (representative examples):
  - [gistformdownlink](gistformdownlink.md)
  - [gistfinishsplit](gistfinishsplit.md)
  - [gistFindCorrectParent](gistFindCorrectParent.md) (recursive)

## Notes and Other Information
- The function requires the child's parent to be exclusively locked on entry and maintains this lock on exit
- Uses different strategies for finding the correct parent, from fast local checks to expensive tree traversals
- Handles the special case where LSN checking behaves differently during index builds
- The function is recursive - it calls itself after reconstructing the parent chain to ensure correctness
- Properly manages buffer references and locks throughout the complex search process
- Critical for maintaining tree consistency in the presence of concurrent page splits and modifications
- The assertion check verifies that either the LSN changed, we're in build mode, or the downlink offset was invalid

## Simplified Source
```c
static void gistFindCorrectParent(Relation r, GISTInsertStack *child, bool is_build) {
    GISTInsertStack *parent = child->parent;
    ItemId iid;
    IndexTuple idxtuple;
    OffsetNumber maxoff;

    // Refresh parent page info
    gistcheckpage(r, parent->buffer);
    parent->page = BufferGetPage(parent->buffer);
    maxoff = PageGetMaxOffsetNumber(parent->page);

    // Quick check: is downlink still at expected offset?
    if (child->downlinkoffnum != InvalidOffsetNumber &&
        child->downlinkoffnum <= maxoff) {
        iid = PageGetItemId(parent->page, child->downlinkoffnum);
        idxtuple = (IndexTuple) PageGetItem(parent->page, iid);
        if (ItemPointerGetBlockNumber(&(idxtuple->t_tid)) == child->blkno)
            return;  // Still there, nothing to do
    }

    // Page changed - validate this is expected
    Assert(parent->lsn != PageGetLSN(parent->page) || is_build ||
           child->downlinkoffnum == InvalidOffsetNumber);

    // Search for downlink in current page and right siblings
    while (true) {
        maxoff = PageGetMaxOffsetNumber(parent->page);

        // Scan current page for the downlink
        for (OffsetNumber i = FirstOffsetNumber; i <= maxoff; i++) {
            iid = PageGetItemId(parent->page, i);
            idxtuple = (IndexTuple) PageGetItem(parent->page, iid);
            if (ItemPointerGetBlockNumber(&(idxtuple->t_tid)) == child->blkno) {
                // Found it!
                child->downlinkoffnum = i;
                return;
            }
        }

        // Not found, try right sibling
        parent->blkno = GistPageGetOpaque(parent->page)->rightlink;
        parent->downlinkoffnum = InvalidOffsetNumber;
        UnlockReleaseBuffer(parent->buffer);

        if (parent->blkno == InvalidBlockNumber) {
            // End of chain - very rare case (root split)
            break;
        }

        // Move to right sibling
        parent->buffer = ReadBuffer(r, parent->blkno);
        LockBuffer(parent->buffer, GIST_EXCLUSIVE);
        gistcheckpage(r, parent->buffer);
        parent->page = BufferGetPage(parent->buffer);
    }

    // Downlink not found in right siblings - need full tree search
    // Release all old parent buffers
    GISTInsertStack *ptr = child->parent->parent;
    while (ptr) {
        ReleaseBuffer(ptr->buffer);
        ptr = ptr->parent;
    }

    // Find new path from root to child
    parent = gistFindPath(r, child->blkno, &child->downlinkoffnum);

    // Read all buffers in new path (but don't lock them yet)
    ptr = parent;
    while (ptr) {
        ptr->buffer = ReadBuffer(r, ptr->blkno);
        ptr->page = BufferGetPage(ptr->buffer);
        ptr = ptr->parent;
    }

    // Install new parent chain
    child->parent = parent;

    // Recursively ensure the new parent chain is correct
    LockBuffer(child->parent->buffer, GIST_EXCLUSIVE);
    gistFindCorrectParent(r, child, is_build);
}
```