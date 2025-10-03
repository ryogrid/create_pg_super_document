# ginVacuumPostingTreeLeaves

## Location
[src/backend/access/gin/ginvacuum.c:346-408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginvacuum.c#L346-L408)

## Overview
Traverses all leaf pages of a GIN posting tree from left to right, vacuuming each leaf page and determining if any empty pages exist.

## Definition

```c
static bool
ginVacuumPostingTreeLeaves(GinVacuumState *gvs, BlockNumber blkno)
```
## Detailed Description
This static function performs a two-phase operation on GIN posting tree leaf pages. First, it navigates down the posting tree to find the leftmost leaf page by following the first posting item in each internal page until reaching a leaf. Then, it traverses all leaf pages from left to right using rightlinks, calling ginVacuumPostingTreeLeaf() to vacuum each page.

The function uses a temporary memory context (gvs->tmpCxt) for each leaf page vacuum operation, which is reset after processing each page to prevent memory bloat during long vacuum operations. It employs careful locking strategies, starting with shared locks during tree descent and upgrading to exclusive locks for actual vacuuming.

## Parameters / Member Variables
- : GinVacuumState containing index context, temporary memory context, buffer strategy, and vacuum state
- : Block number of the root or starting page of the posting tree to begin traversal

## Dependencies
- Functions called/Symbols referenced:
  - [ReadBufferExtended](../R/ReadBufferExtended.md) (read pages into buffers)
  - [LockBuffer](../L/LockBuffer.md) (acquire/release buffer locks with GIN_SHARE, GIN_EXCLUSIVE, GIN_UNLOCK)
  - [BufferGetPage](../B/BufferGetPage.md) (get page from buffer)
  - GinPageIsData/GinPageIsLeaf (page type verification)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md) (get maximum offset)
  - GinDataPageGetPostingItem (get posting item from page)
  - PostingItemGetBlockNumber (extract block number from posting item)
  - [ginVacuumPostingTreeLeaf](ginVacuumPostingTreeLeaf.md) (vacuum individual leaf page)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)/MemoryContextReset (memory context management)
  - GinDataLeafPageIsEmpty (check if leaf page is empty)
  - GinPageGetOpaque (access page opaque data for rightlink)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md) (release buffer and lock)
- Called from (representative examples):
  - [ginVacuumPostingTree](ginVacuumPostingTree.md)

## Notes and Other Information
- Static function, only accessible within ginvacuum.c
- Returns true if at least one empty page is found during traversal
- Uses lock upgrade strategy: shared lock for descent, exclusive lock for vacuuming
- Manages memory efficiently by resetting temporary context after each leaf page
- Traverses leaf pages using rightlinks, terminating when rightlink is InvalidBlockNumber
- Essential for the leaf-level vacuum phase before potential page deletion operations
- Follows standard PostgreSQL buffer management and locking protocols

## Simplified Source

```c
static bool
ginVacuumPostingTreeLeaves(GinVacuumState *gvs, BlockNumber blkno)
{
    Buffer buffer;
    Page page;
    bool hasVoidPage = false;
    MemoryContext oldCxt;

    // Navigate down to leftmost leaf page
    while (true) {
        PostingItem *pitem;

        buffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, blkno,
                                   RBM_NORMAL, gvs->strategy);
        LockBuffer(buffer, GIN_SHARE);
        page = BufferGetPage(buffer);

        // If we reached a leaf page, upgrade to exclusive lock and break
        if (GinPageIsLeaf(page)) {
            LockBuffer(buffer, GIN_UNLOCK);
            LockBuffer(buffer, GIN_EXCLUSIVE);
            break;
        }

        // Follow first posting item to descend further
        pitem = GinDataPageGetPostingItem(page, FirstOffsetNumber);
        blkno = PostingItemGetBlockNumber(pitem);

        UnlockReleaseBuffer(buffer);
    }

    // Traverse all leaf pages from left to right
    while (true) {
        // Switch to temporary context for vacuum operation
        oldCxt = MemoryContextSwitchTo(gvs->tmpCxt);
        ginVacuumPostingTreeLeaf(gvs->index, buffer, gvs);
        MemoryContextSwitchTo(oldCxt);
        MemoryContextReset(gvs->tmpCxt);

        // Check if this page is empty
        if (GinDataLeafPageIsEmpty(page))
            hasVoidPage = true;

        // Move to next leaf page via rightlink
        blkno = GinPageGetOpaque(page)->rightlink;
        UnlockReleaseBuffer(buffer);

        // End of leaf chain
        if (blkno == InvalidBlockNumber)
            break;

        // Read next leaf page
        buffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, blkno,
                                   RBM_NORMAL, gvs->strategy);
        LockBuffer(buffer, GIN_EXCLUSIVE);
        page = BufferGetPage(buffer);
    }

    return hasVoidPage;
}
```