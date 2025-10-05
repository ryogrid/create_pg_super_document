# btree_xlog_mark_page_halfdead

## Location
[src/backend/access/nbtree/nbtxlog.c:713-797](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtxlog.c#L713-L797)

## Overview
Replays WAL records that mark a B-tree page as half-dead during page deletion, updating the parent page and reinitializing the target page.

## Definition

```c
struct a dummy high key item that points to top parent page (value
	 * is InvalidBlockNumber when the top parent page is the leaf page itself)
	 */
	MemSet(&trunctuple, 0, sizeof(IndexTupleData));
```
## Detailed Description
This function handles the recovery/replay of B-tree page half-dead marking operations from WAL records. The half-dead state is an intermediate step in B-tree page deletion where a page is marked for deletion but not yet physically removed from the tree structure.

The function performs two main operations:
1. Updates the parent page by removing the downlink to the page being deleted and adjusting the remaining downlinks
2. Reinitializes the target page as a half-dead page with special opaque data

The half-dead page contains only a dummy high key that points to the top parent page in the deletion chain. This allows concurrent scans to navigate correctly while the deletion process completes.

Key operations performed:
1. Updates the parent page by modifying downlinks and deleting the reference to the half-dead page
2. Reinitializes the target page with _bt_pageinit
3. Sets the page opaque flags to BTP_HALF_DEAD | BTP_LEAF
4. Creates a dummy high key tuple pointing to the top parent page
5. Updates page LSNs and marks buffers dirty

## Parameters / Member Variables
- `info`: WAL record info byte (currently unused)
- `*record`: XLogReaderState containing the WAL record data for the mark half-dead operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md)
  - BTPageGetOpaque
  - [BTreeTupleGetDownLink](../B/BTreeTupleGetDownLink.md)
  - [BTreeTupleSetDownLink](../B/BTreeTupleSetDownLink.md)
  - [BTreeTupleSetTopParent](../B/BTreeTupleSetTopParent.md)
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md)
  - [_bt_pageinit](_bt_pageinit.md)
  - PageAddItem
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
- Called from (representative examples):
  - [btree_redo](btree_redo.md)

## Notes and Other Information
- This is a static function used internally for B-tree WAL recovery
- Part of the multi-step B-tree page deletion process
- The half-dead state allows safe concurrent access during page deletion
- Locks pages individually rather than using complex cross-level locking during recovery
- The dummy high key enables proper navigation for concurrent scans
- Critical for maintaining B-tree structure integrity during page deletion recovery
- Works in conjunction with other deletion-related WAL operations like unlink and delete

## Simplified Source

```c
static void
btree_xlog_mark_page_halfdead(uint8 info, XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_btree_mark_page_halfdead *xlrec = (xl_btree_mark_page_halfdead *) XLogRecGetData(record);
    Buffer buffer;
    Page page;
    BTPageOpaque pageop;
    IndexTupleData trunctuple;

    // Update parent page - remove downlink to deleted page
    if (XLogReadBufferForRedo(record, 1, &buffer) == BLK_NEEDS_REDO)
    {
        OffsetNumber poffset;
        ItemId itemid;
        IndexTuple itup;
        OffsetNumber nextoffset;
        BlockNumber rightsib;

        page = (Page) BufferGetPage(buffer);
        poffset = xlrec->poffset;

        // Get right sibling from next item
        nextoffset = OffsetNumberNext(poffset);
        itemid = PageGetItemId(page, nextoffset);
        itup = (IndexTuple) PageGetItem(page, itemid);
        rightsib = BTreeTupleGetDownLink(itup);

        // Update parent's downlink to skip deleted page
        itemid = PageGetItemId(page, poffset);
        itup = (IndexTuple) PageGetItem(page, itemid);
        BTreeTupleSetDownLink(itup, rightsib);

        // Delete the next item (pointing to deleted page)
        PageIndexTupleDelete(page, nextoffset);

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    if (BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);

    // Reinitialize target page as half-dead
    buffer = XLogInitBufferForRedo(record, 0);
    page = (Page) BufferGetPage(buffer);

    _bt_pageinit(page, BufferGetPageSize(buffer));
    pageop = BTPageGetOpaque(page);

    // Set page metadata
    pageop->btpo_prev = xlrec->leftblk;
    pageop->btpo_next = xlrec->rightblk;
    pageop->btpo_level = 0;
    pageop->btpo_flags = BTP_HALF_DEAD | BTP_LEAF;
    pageop->btpo_cycleid = 0;

    // Create dummy high key pointing to top parent
    MemSet(&trunctuple, 0, sizeof(IndexTupleData));
    trunctuple.t_info = sizeof(IndexTupleData);
    BTreeTupleSetTopParent(&trunctuple, xlrec->topparent);

    if (PageAddItem(page, (Item) &trunctuple, sizeof(IndexTupleData), P_HIKEY, false, false) == InvalidOffsetNumber)
        elog(ERROR, "could not add dummy high key to half-dead page");

    PageSetLSN(page, lsn);
    MarkBufferDirty(buffer);
    UnlockReleaseBuffer(buffer);
}
```