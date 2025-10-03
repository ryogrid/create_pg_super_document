# gistFindPath

## Location
[src/backend/access/gist/gist.c:909-1021](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gist.c#L909-L1021)

## Overview
Traverses the GiST tree to find the path from the root page to a specified child block, returning a stack of pages representing the path from parent to root.

## Definition

```c
static GISTInsertStack *
gistFindPath(Relation r, BlockNumber child, OffsetNumber *downlinkoffnum)
```
## Detailed Description
 performs a breadth-first search from the root of the GiST tree to locate a specific child block and construct the path back to the root. The function is primarily used for recovery operations when the parent-child relationship needs to be re-established.

The algorithm works by:
1. Starting from the root page and maintaining a FIFO queue of pages to visit
2. For each internal page, scanning all downlink tuples to find children 
3. If the target child is found, returning the insertion stack path
4. Otherwise, adding all child pages to the queue for further exploration
5. Handling concurrent page splits by detecting them via LSN comparison and adding newly split pages to the queue

The function implements deadlock prevention by locking only one page at a time and uses shared locks throughout the traversal.

## Parameters / Member Variables
- `r`: The GiST index relation to search
- `child`: The block number of the target child page to find
- `*downlinkoffnum`: Output parameter set to the offset number of the downlink tuple in the direct parent that points to the child
## Dependencies
- Functions called/Symbols referenced:
  - [gistcheckpage](gistcheckpage.md)
  - GistFollowRight
  - GistPageGetNSN
  - GistPageGetOpaque
  - GistPageIsDeleted
  - GistPageIsLeaf
  - [BufferGetLSNAtomic](../B/BufferGetLSNAtomic.md)
  - [ReadBuffer](../R/ReadBuffer.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - list_make1
  - [list_delete_first](../l/list_delete_first.md)
  - [lcons](../l/lcons.md)
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [gistFindCorrectParent](gistFindCorrectParent.md)

## Notes and Other Information
- Uses breadth-first search rather than depth-first to ensure leaf pages are encountered after all internal pages
- Includes special handling for concurrent page splits detected via LSN-NSN comparison
- The function assumes internal pages are never deleted (assertion check)
- Detects incomplete page splits and reports them as errors
- Returns NULL on failure but throws an ERROR if the child page cannot be found
- The returned insertion stack represents the path from the direct parent of the target child up to the root
- Uses a FIFO queue implemented as a PostgreSQL List to manage the breadth-first traversal

## Simplified Source
```c
static GISTInsertStack *
gistFindPath(Relation r, BlockNumber child, OffsetNumber *downlinkoffnum) {
    Page page;
    Buffer buffer;
    List *fifo;
    GISTInsertStack *top, *ptr;
    BlockNumber blkno;

    // Initialize search from root
    top = (GISTInsertStack *) palloc0(sizeof(GISTInsertStack));
    top->blkno = GIST_ROOT_BLKNO;
    top->downlinkoffnum = InvalidOffsetNumber;

    // Use FIFO queue for breadth-first search
    fifo = list_make1(top);

    while (fifo != NIL) {
        // Get next page to examine
        top = linitial(fifo);
        fifo = list_delete_first(fifo);

        // Read and lock page
        buffer = ReadBuffer(r, top->blkno);
        LockBuffer(buffer, GIST_SHARE);
        gistcheckpage(r, buffer);
        page = BufferGetPage(buffer);

        // Stop at leaf level - all remaining pages must be leaves
        if (GistPageIsLeaf(page)) {
            UnlockReleaseBuffer(buffer);
            break;
        }

        Assert(!GistPageIsDeleted(page));  // Internal pages never deleted
        top->lsn = BufferGetLSNAtomic(buffer);

        // Check for incomplete splits
        if (GistFollowRight(page))
            elog(ERROR, "concurrent GiST page split was incomplete");

        // Handle concurrent page splits - add right sibling if split detected
        if (top->parent && top->parent->lsn < GistPageGetNSN(page) &&
            GistPageGetOpaque(page)->rightlink != InvalidBlockNumber) {
            ptr = (GISTInsertStack *) palloc0(sizeof(GISTInsertStack));
            ptr->blkno = GistPageGetOpaque(page)->rightlink;
            ptr->downlinkoffnum = InvalidOffsetNumber;
            ptr->parent = top->parent;
            fifo = lcons(ptr, fifo);  // Add to front of queue
        }

        // Scan all downlink tuples on this page
        OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
        for (OffsetNumber i = FirstOffsetNumber; i <= maxoff; i++) {
            ItemId iid = PageGetItemId(page, i);
            IndexTuple idxtuple = (IndexTuple) PageGetItem(page, iid);
            blkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));

            if (blkno == child) {
                // Found target child!
                UnlockReleaseBuffer(buffer);
                *downlinkoffnum = i;
                return top;
            } else {
                // Add child to queue for later exploration
                ptr = (GISTInsertStack *) palloc0(sizeof(GISTInsertStack));
                ptr->blkno = blkno;
                ptr->downlinkoffnum = i;
                ptr->parent = top;
                fifo = lappend(fifo, ptr);
            }
        }

        UnlockReleaseBuffer(buffer);
    }

    // Child not found - this is an error condition
    elog(ERROR, "failed to re-find parent of page in index \"%s\", block %u",
         RelationGetRelationName(r), child);
    return NULL;
}
```