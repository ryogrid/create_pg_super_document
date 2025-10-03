# ginPlaceToPage

## Location
[src/backend/access/gin/ginbtree.c:337-671](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginbtree.c#L337-L671)

## Overview
ginPlaceToPage handles the insertion of new items to a GIN B-tree page, managing page splits when necessary and maintaining B-tree consistency through proper WAL logging and locking.

## Definition

```c
struct a new root page containing downlinks to the new left
			 * and right pages.  (Do this in a temporary copy rather than
			 * overwriting the original page directly, since we're not in the
			 * critical section yet.)
			 */
			newrootpg = PageGetTempPage(newrpage);
```
## Detailed Description
ginPlaceToPage is the core insertion routine for GIN B-tree pages that determines whether a new item can fit on the target page or if a split is required. The function operates in three phases:

1. **Fit Analysis**: Uses the btree's beginPlaceToPage callback to determine if the insertion requires a split
2. **Simple Insertion**: If the item fits, performs direct insertion using execPlaceToPage callback
3. **Page Split**: If the item doesn't fit, allocates new pages and redistributes content, handling both regular splits and root splits

The function maintains ACID properties through proper use of critical sections and WAL logging. When inserting downlinks to internal pages, it atomically clears the GIN_INCOMPLETE_SPLIT flag on child pages. The function operates within a temporary memory context to avoid memory leaks during complex split operations.

## Parameters / Member Variables
- : GinBtree structure containing method pointers and index metadata
- : GinBtreeStack representing the current position in the B-tree traversal
- : Data payload to be inserted (format depends on page type)
- : Block number for updating existing downlinks (internal pages only)  
- : Buffer containing child page being split (for internal page insertions)
- : Statistics tracking structure used during index builds

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [BufferGetPage](../B/BufferGetPage.md), BufferGetBlockNumber
  - GinPageIsData, GinPageIsLeaf, GinPageGetOpaque
  - [GinNewBuffer](../G/GinNewBuffer.md), GinInitPage
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterBuffer, XLogInsert
  - [PageGetTempPage](../P/PageGetTempPage.md), PredicateLockPageSplit
  - START_CRIT_SECTION, END_CRIT_SECTION
- Called from:
  - [ginFinishSplit](ginFinishSplit.md) (src/backend/access/gin/ginbtree.c:736)
  - [ginInsertValue](ginInsertValue.md) (src/backend/access/gin/ginbtree.c:825)

## Notes and Other Information
The function returns true when insertion is complete, false when a parent update is needed after a split. Root splits always return true since they don't require further parent updates. The function handles both data pages and entry pages, with different WAL record types (XLOG_GIN_INSERT vs XLOG_GIN_SPLIT). Memory management uses a temporary context to ensure cleanup of intermediate allocations during split operations.

## Simplified Source

```c
static bool
ginPlaceToPage(GinBtree btree, GinBtreeStack *stack,
               void *insertdata, BlockNumber updateblkno,
               Buffer childbuf, GinStatsData *buildStats)
{
    Page page = BufferGetPage(stack->buffer);
    bool result;
    GinPlaceToPageRC rc;
    uint16 xlflags = 0;
    Page childpage = NULL;
    Page newlpage = NULL, newrpage = NULL;
    void *ptp_workspace = NULL;

    // Work in temporary memory context
    MemoryContext tmpCxt = AllocSetContextCreate(CurrentMemoryContext,
                                                "ginPlaceToPage temporary context",
                                                ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldCxt = MemoryContextSwitchTo(tmpCxt);

    // Set WAL flags based on page type
    if (GinPageIsData(page))
        xlflags |= GIN_INSERT_ISDATA;
    if (GinPageIsLeaf(page))
        xlflags |= GIN_INSERT_ISLEAF;
    else
        childpage = BufferGetPage(childbuf);

    // Check if insertion fits or requires split
    rc = btree->beginPlaceToPage(btree, stack->buffer, stack,
                                insertdata, updateblkno,
                                &ptp_workspace,
                                &newlpage, &newrpage);

    if (rc == GPTP_NO_WORK)
    {
        result = true; // Nothing to do
    }
    else if (rc == GPTP_INSERT)
    {
        // Simple insertion - fits on page
        START_CRIT_SECTION();

        if (RelationNeedsWAL(btree->index) && !btree->isBuild)
            XLogBeginInsert();

        // Execute the insertion
        btree->execPlaceToPage(btree, stack->buffer, stack,
                              insertdata, updateblkno, ptp_workspace);

        // Clear incomplete split flag on child if present
        if (BufferIsValid(childbuf))
        {
            GinPageGetOpaque(childpage)->flags &= ~GIN_INCOMPLETE_SPLIT;
            MarkBufferDirty(childbuf);
        }

        // Write WAL record
        if (RelationNeedsWAL(btree->index) && !btree->isBuild)
        {
            // Log insert operation
            XLogRecPtr recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_INSERT);
            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();
        result = true;
    }
    else if (rc == GPTP_SPLIT)
    {
        // Page split required
        Buffer rbuffer = GinNewBuffer(btree->index);
        BlockNumber savedRightLink = GinPageGetOpaque(page)->rightlink;
        Buffer lbuffer = InvalidBuffer;
        Page newrootpg = NULL;

        if (stack->parent == NULL)
        {
            // Root split - create new left page and update root
            lbuffer = GinNewBuffer(btree->index);

            GinPageGetOpaque(newlpage)->rightlink = BufferGetBlockNumber(rbuffer);

            // Create new root page
            newrootpg = PageGetTempPage(newrpage);
            GinInitPage(newrootpg, GinPageGetOpaque(newlpage)->flags & ~(GIN_LEAF | GIN_COMPRESSED), BLCKSZ);

            btree->fillRoot(btree, newrootpg,
                           BufferGetBlockNumber(lbuffer), newlpage,
                           BufferGetBlockNumber(rbuffer), newrpage);
        }
        else
        {
            // Normal split
            GinPageGetOpaque(newrpage)->rightlink = savedRightLink;
            GinPageGetOpaque(newlpage)->flags |= GIN_INCOMPLETE_SPLIT;
            GinPageGetOpaque(newlpage)->rightlink = BufferGetBlockNumber(rbuffer);
        }

        START_CRIT_SECTION();

        // Update pages
        if (stack->parent == NULL)
        {
            // Root split: update three pages
            memcpy(page, newrootpg, BLCKSZ);
            memcpy(BufferGetPage(lbuffer), newlpage, BLCKSZ);
            memcpy(BufferGetPage(rbuffer), newrpage, BLCKSZ);
        }
        else
        {
            // Normal split: update two pages
            memcpy(page, newlpage, BLCKSZ);
            memcpy(BufferGetPage(rbuffer), newrpage, BLCKSZ);
        }

        // Clear child incomplete split flag
        if (BufferIsValid(childbuf))
        {
            GinPageGetOpaque(childpage)->flags &= ~GIN_INCOMPLETE_SPLIT;
            MarkBufferDirty(childbuf);
        }

        // Write WAL record for split
        if (RelationNeedsWAL(btree->index) && !btree->isBuild)
        {
            XLogRecPtr recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_SPLIT);
            PageSetLSN(page, recptr);
            PageSetLSN(BufferGetPage(rbuffer), recptr);
        }

        END_CRIT_SECTION();

        // Release new buffers
        UnlockReleaseBuffer(rbuffer);
        if (stack->parent == NULL)
            UnlockReleaseBuffer(lbuffer);

        result = (stack->parent == NULL); // Root split is complete
    }

    // Clean up temporary context
    MemoryContextSwitchTo(oldCxt);
    MemoryContextDelete(tmpCxt);

    return result;
}
```