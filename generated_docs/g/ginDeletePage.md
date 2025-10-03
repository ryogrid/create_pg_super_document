# ginDeletePage

## Location
[src/backend/access/gin/ginvacuum.c:130-246](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginvacuum.c#L130-L246)

## Overview
Deletes a posting tree page from a GIN index by unlinking it from the tree structure and marking it as deleted for eventual reuse.

## Definition

```c
static void
ginDeletePage(GinVacuumState *gvs, BlockNumber deleteBlkno, BlockNumber leftBlkno,
			  BlockNumber parentBlkno, OffsetNumber myoff, bool isParentRoot)
```
## Detailed Description
This static function performs the complete deletion of a posting tree page in a GIN index. It handles the complex process of safely removing a page from the B-tree structure while maintaining consistency and crash recovery. The function requires that the parent page holds an exclusive cleanup lock to guarantee no concurrent insertions occur in the subtree during deletion.

The deletion process involves multiple steps: unlinking the page from its siblings by updating the left sibling's rightlink, removing the downlink from the parent page, marking the page as deleted with a transaction ID, and creating appropriate WAL records for crash recovery. The function also handles predicate locking to ensure that any inserts that would have gone to the deleted page are redirected to its right sibling.

## Parameters / Member Variables
- `*gvs`: GinVacuumState containing index context, buffer strategy, and result statistics
- `deleteBlkno`: Block number of the page to be deleted
- `leftBlkno`: Block number of the left sibling page that needs rightlink update
- `parentBlkno`: Block number of the parent page containing the downlink to remove
- `myoff`: Offset in the parent page of the downlink pointing to the page being deleted
- `isParentRoot`: Boolean indicating whether the parent page is the root (currently unused in function body)
## Dependencies
- Functions called/Symbols referenced:
  - [ReadBufferExtended](../R/ReadBufferExtended.md) (read pages into buffers)
  - [BufferGetPage](../B/BufferGetPage.md) (get page from buffer)
  - GinPageGetOpaque (access page opaque data)
  - [PredicateLockPageCombine](../P/PredicateLockPageCombine.md) (handle predicate locking)
  - GinDataPageGetPostingItem (get posting item from page)
  - PostingItemGetBlockNumber (extract block number from posting item)
  - [GinPageDeletePostingItem](../G/GinPageDeletePostingItem.md) (remove posting item from page)
  - GinPageSetDeleted (mark page as deleted)
  - GinPageSetDeleteXid (set deletion transaction ID)
  - [ReadNextTransactionId](../R/ReadNextTransactionId.md) (get next transaction ID)
  - [MarkBufferDirty](../M/MarkBufferDirty.md) (mark buffers as modified)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)/XLogRegisterBuffer/XLogInsert (WAL logging)
  - [ReleaseBuffer](../R/ReleaseBuffer.md) (release buffer references)
- Called from (representative examples):
  - [ginScanToDelete](ginScanToDelete.md)

## Notes and Other Information
- Static function, only accessible within ginvacuum.c
- Requires exclusive cleanup lock on parent page before calling
- Updates vacuum statistics (pages_newly_deleted, pages_deleted)
- Handles special WAL registration due to pd_lower issues in pre-9.4 binary-upgraded pages
- Preserves rightlink in deleted page to maintain workability of running search scans
- Uses critical section to ensure atomicity of the deletion operation
- Includes debug assertions to verify the correct posting item is being deleted

## Simplified Source

```c
static void
ginDeletePage(GinVacuumState *gvs, BlockNumber deleteBlkno, BlockNumber leftBlkno,
              BlockNumber parentBlkno, OffsetNumber myoff, bool isParentRoot)
{
    Buffer dBuffer, lBuffer, pBuffer;
    Page page, parentPage;
    BlockNumber rightlink;

    // Read the three pages involved: page to delete, left sibling, parent
    lBuffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, leftBlkno,
                                RBM_NORMAL, gvs->strategy);
    dBuffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, deleteBlkno,
                                RBM_NORMAL, gvs->strategy);
    pBuffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, parentBlkno,
                                RBM_NORMAL, gvs->strategy);

    // Get the right link from the page being deleted
    page = BufferGetPage(dBuffer);
    rightlink = GinPageGetOpaque(page)->rightlink;

    // Handle predicate locking for concurrent scans
    PredicateLockPageCombine(gvs->index, deleteBlkno, rightlink);

    START_CRIT_SECTION();

    // Step 1: Update left sibling to point to right sibling
    page = BufferGetPage(lBuffer);
    GinPageGetOpaque(page)->rightlink = rightlink;

    // Step 2: Remove downlink from parent page
    parentPage = BufferGetPage(pBuffer);
    GinPageDeletePostingItem(parentPage, myoff);

    // Step 3: Mark the page as deleted with current transaction ID
    page = BufferGetPage(dBuffer);
    GinPageSetDeleted(page);
    GinPageSetDeleteXid(page, ReadNextTransactionId());

    // Mark all buffers dirty for WAL
    MarkBufferDirty(pBuffer);
    MarkBufferDirty(lBuffer);
    MarkBufferDirty(dBuffer);

    // Write WAL record if needed
    if (RelationNeedsWAL(gvs->index)) {
        XLogRecPtr recptr;
        ginxlogDeletePage data;

        XLogBeginInsert();
        XLogRegisterBuffer(0, dBuffer, 0);
        XLogRegisterBuffer(1, pBuffer, REGBUF_STANDARD);
        XLogRegisterBuffer(2, lBuffer, 0);

        data.parentOffset = myoff;
        data.rightLink = rightlink;
        data.deleteXid = GinPageGetDeleteXid(page);

        XLogRegisterData((char *) &data, sizeof(ginxlogDeletePage));
        recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_DELETE_PAGE);

        // Set LSN on all modified pages
        PageSetLSN(page, recptr);
        PageSetLSN(parentPage, recptr);
        PageSetLSN(BufferGetPage(lBuffer), recptr);
    }

    // Release all buffers
    ReleaseBuffer(pBuffer);
    ReleaseBuffer(lBuffer);
    ReleaseBuffer(dBuffer);

    END_CRIT_SECTION();

    // Update vacuum statistics
    gvs->result->pages_newly_deleted++;
    gvs->result->pages_deleted++;
}
```