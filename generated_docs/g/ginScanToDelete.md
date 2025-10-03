# ginScanToDelete

## Location
[src/backend/access/gin/ginvacuum.c:247-345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginvacuum.c#L247-L345)

## Overview
Recursively scans a GIN posting tree to identify and delete empty pages while maintaining proper locking to prevent deadlocks and ensure consistency.

## Definition

```c
static bool
ginScanToDelete(GinVacuumState *gvs, BlockNumber blkno, bool isRoot,
				DataPageDeleteStack *parent, OffsetNumber myoff)
```
## Detailed Description
This recursive static function traverses a GIN posting tree to locate and delete empty pages. It implements a sophisticated locking protocol that maintains exclusive locks on the path from root to current page and keeps the left sibling locked to avoid deadlocks with concurrent ginStepRight() operations. The function uses a DataPageDeleteStack structure to track the deletion context and parent-child relationships during traversal.

For internal pages, the function recursively processes all child pages. For leaf pages, it uses GinDataLeafPageIsEmpty() to determine emptiness, while for internal pages it checks if maxoff is less than FirstOffsetNumber. Empty pages are only deleted if they have a valid left sibling and are not the rightmost page, ensuring tree structure integrity.

## Parameters / Member Variables
- `*gvs`: GinVacuumState containing index context, buffer strategy, and vacuum statistics
- `blkno`: Block number of the current page being scanned
- `isRoot`: Boolean indicating whether the current page is the root of the posting tree
- `*parent`: DataPageDeleteStack pointer representing the parent context in the deletion stack
- `myoff`: Offset in the parent page that points to the current page (used for deletion)
## Dependencies
- Functions called/Symbols referenced:
  - [DataPageDeleteStack](../D/DataPageDeleteStack.md) (struct for tracking deletion context)
  - [ReadBufferExtended](../R/ReadBufferExtended.md) (read page into buffer)
  - [LockBuffer](../L/LockBuffer.md) (acquire/release buffer locks)
  - [BufferGetPage](../B/BufferGetPage.md) (get page from buffer)
  - GinPageIsData/GinPageIsLeaf (page type checks)
  - GinPageGetOpaque (access page opaque data)
  - GinDataPageGetPostingItem (get posting item from page)
  - PostingItemGetBlockNumber (extract block number)
  - GinPageRightMost (check if page is rightmost)
  - GinDataLeafPageIsEmpty (check if leaf page is empty)
  - [ginDeletePage](ginDeletePage.md) (delete empty page)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (get block number from buffer)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)/ReleaseBuffer (buffer management)
- Called from (representative examples):
  - [ginScanToDelete](ginScanToDelete.md) (recursive self-call)
  - [ginVacuumPostingTree](ginVacuumPostingTree.md)

## Notes and Other Information
- Static function, only accessible within ginvacuum.c
- Implements recursive tree traversal with careful lock management
- Uses DataPageDeleteStack to maintain deletion context across recursive calls
- Never deletes leftmost or rightmost pages to preserve tree structure
- Returns true if the current page was deleted, false otherwise
- Reuses DataPageDeleteStack structures to minimize memory allocation
- Handles special unlocking of leftBuffer when reaching rightmost pages
- Critical for maintaining posting tree integrity during vacuum operations

## Simplified Source

```c
static bool
ginScanToDelete(GinVacuumState *gvs, BlockNumber blkno, bool isRoot,
                DataPageDeleteStack *parent, OffsetNumber myoff)
{
    DataPageDeleteStack *me;
    Buffer buffer;
    Page page;
    bool meDelete = false;
    bool isempty;

    // Setup deletion stack entry (reuse existing or create new)
    if (isRoot) {
        me = parent;
    } else {
        if (!parent->child) {
            me = (DataPageDeleteStack *) palloc0(sizeof(DataPageDeleteStack));
            me->parent = parent;
            parent->child = me;
            me->leftBuffer = InvalidBuffer;
        } else {
            me = parent->child;
        }
    }

    // Read and lock the current page
    buffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, blkno,
                                RBM_NORMAL, gvs->strategy);
    if (!isRoot)
        LockBuffer(buffer, GIN_EXCLUSIVE);

    page = BufferGetPage(buffer);

    // For internal pages, recursively process all children
    if (!GinPageIsLeaf(page)) {
        OffsetNumber i;
        me->blkno = blkno;

        for (i = FirstOffsetNumber; i <= GinPageGetOpaque(page)->maxoff; i++) {
            PostingItem *pitem = GinDataPageGetPostingItem(page, i);
            // Recursive call - if child was deleted, decrement i to reprocess
            if (ginScanToDelete(gvs, PostingItemGetBlockNumber(pitem), false, me, i))
                i--;
        }

        // Handle rightmost page cleanup
        if (GinPageRightMost(page) && BufferIsValid(me->child->leftBuffer)) {
            UnlockReleaseBuffer(me->child->leftBuffer);
            me->child->leftBuffer = InvalidBuffer;
        }
    }

    // Check if page is empty
    if (GinPageIsLeaf(page))
        isempty = GinDataLeafPageIsEmpty(page);
    else
        isempty = GinPageGetOpaque(page)->maxoff < FirstOffsetNumber;

    // Delete page if empty and safe to delete
    if (isempty) {
        // Never delete leftmost or rightmost pages
        if (BufferIsValid(me->leftBuffer) && !GinPageRightMost(page)) {
            ginDeletePage(gvs, blkno, BufferGetBlockNumber(me->leftBuffer),
                         me->parent->blkno, myoff, me->parent->isRoot);
            meDelete = true;
        }
    }

    // Manage leftBuffer for next iteration
    if (!meDelete) {
        if (BufferIsValid(me->leftBuffer))
            UnlockReleaseBuffer(me->leftBuffer);
        me->leftBuffer = buffer;
    } else {
        if (!isRoot)
            LockBuffer(buffer, GIN_UNLOCK);
        ReleaseBuffer(buffer);
    }

    if (isRoot)
        ReleaseBuffer(buffer);

    return meDelete;
}
```