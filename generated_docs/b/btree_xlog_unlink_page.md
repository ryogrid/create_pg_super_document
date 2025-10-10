# btree_xlog_unlink_page

## Location
[src/backend/access/nbtree/nbtxlog.c:798-936](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtxlog.c#L798-L936)

## Overview
Replays a B-tree page unlink operation during WAL recovery, updating sibling links and marking the target page as deleted.

## Definition

```c
static void
btree_xlog_unlink_page(uint8 info, XLogReaderState *record)
```

## Detailed Description
This function handles the replay of B-tree page unlink operations during Write-Ahead Log (WAL) recovery. When a B-tree page is deleted during normal operation, the page must be unlinked from the B-tree structure by updating the left and right sibling pointers. During recovery, this function reconstructs these changes from the WAL record.

The function performs several key operations:
1. Updates the right-link of the left sibling page (if it exists) to point to the right sibling
2. Reinitializes the target page as an empty deleted page with proper opaque data
3. Updates the left-link of the right sibling page to point to the left sibling
4. If unlinking a parent page (not a leaf), updates the leaf page to maintain the deletion chain
5. Updates the metapage if this is a meta-updating unlink operation

The function carefully follows the same left-to-right locking order used during normal operation to maintain consistency during recovery.

## Parameters / Member Variables
- `info`: WAL record info byte containing operation subtype (XLOG_BTREE_UNLINK_PAGE or XLOG_BTREE_UNLINK_PAGE_META)
- `record`: XLogReaderState containing the WAL record data and block references for the pages involved

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md)
  - XLogRecHasBlockRef
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - BTPageGetOpaque
  - [BTPageSetDeleted](../B/BTPageSetDeleted.md)
  - [BTreeTupleSetTopParent](../B/BTreeTupleSetTopParent.md)
  - [_bt_pageinit](_bt_pageinit.md)
  - [_bt_restore_meta](_bt_restore_meta.md)
  - PageAddItem
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Called from (representative examples):
  - [btree_redo](btree_redo.md)

## Notes and Other Information
- Handles both regular page unlinks and meta-updating unlinks (when info == XLOG_BTREE_UNLINK_PAGE_META)
- Supports unlinking of internal pages by maintaining leaf page chain with dummy hikey items
- The safexid from the WAL record is used to mark the deleted page for safe recycling
- Block references in the WAL record: [0] target page, [1] left sibling, [2] right sibling, [3] leaf page (optional), [4] metapage (optional)
- The function assumes proper locking order is maintained during replay to avoid deadlocks

## Simplified Source

```c
static void
btree_xlog_unlink_page(uint8 info, XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_btree_unlink_page *xlrec = (xl_btree_unlink_page *) XLogRecGetData(record);
    BlockNumber leftsib = xlrec->leftsib;
    BlockNumber rightsib = xlrec->rightsib;
    uint32 level = xlrec->level;
    bool isleaf = (level == 0);
    FullTransactionId safexid = xlrec->safexid;
    Buffer leftbuf, target, rightbuf;
    Page page;
    BTPageOpaque pageop;

    // Fix right-link of left sibling (if exists)
    if (leftsib != P_NONE) {
        if (XLogReadBufferForRedo(record, 1, &leftbuf) == BLK_NEEDS_REDO) {
            page = (Page) BufferGetPage(leftbuf);
            pageop = BTPageGetOpaque(page);
            pageop->btpo_next = rightsib;
            PageSetLSN(page, lsn);
            MarkBufferDirty(leftbuf);
        }
    } else {
        leftbuf = InvalidBuffer;
    }

    // Rewrite target page as empty deleted page
    target = XLogInitBufferForRedo(record, 0);
    page = (Page) BufferGetPage(target);
    _bt_pageinit(page, BufferGetPageSize(target));

    pageop = BTPageGetOpaque(page);
    pageop->btpo_prev = leftsib;
    pageop->btpo_next = rightsib;
    pageop->btpo_level = level;
    BTPageSetDeleted(page, safexid);
    if (isleaf) {
        pageop->btpo_flags |= BTP_LEAF;
    }
    pageop->btpo_cycleid = 0;

    PageSetLSN(page, lsn);
    MarkBufferDirty(target);

    // Fix left-link of right sibling
    if (XLogReadBufferForRedo(record, 2, &rightbuf) == BLK_NEEDS_REDO) {
        page = (Page) BufferGetPage(rightbuf);
        pageop = BTPageGetOpaque(page);
        pageop->btpo_prev = leftsib;
        PageSetLSN(page, lsn);
        MarkBufferDirty(rightbuf);
    }

    // Release sibling pages
    if (BufferIsValid(leftbuf)) {
        UnlockReleaseBuffer(leftbuf);
    }
    if (BufferIsValid(rightbuf)) {
        UnlockReleaseBuffer(rightbuf);
    }
    UnlockReleaseBuffer(target);

    // Update leaf page if unlinking internal page
    if (XLogRecHasBlockRef(record, 3)) {
        Buffer leafbuf;
        IndexTupleData trunctuple;

        leafbuf = XLogInitBufferForRedo(record, 3);
        page = (Page) BufferGetPage(leafbuf);
        _bt_pageinit(page, BufferGetPageSize(leafbuf));

        pageop = BTPageGetOpaque(page);
        pageop->btpo_flags = BTP_HALF_DEAD | BTP_LEAF;
        pageop->btpo_prev = xlrec->leafleftsib;
        pageop->btpo_next = xlrec->leafrightsib;
        pageop->btpo_level = 0;
        pageop->btpo_cycleid = 0;

        // Add dummy hikey item
        MemSet(&trunctuple, 0, sizeof(IndexTupleData));
        trunctuple.t_info = sizeof(IndexTupleData);
        BTreeTupleSetTopParent(&trunctuple, xlrec->leaftopparent);

        if (PageAddItem(page, (Item) &trunctuple, sizeof(IndexTupleData), P_HIKEY, false, false) == InvalidOffsetNumber) {
            elog(ERROR, "could not add dummy high key to half-dead page");
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(leafbuf);
        UnlockReleaseBuffer(leafbuf);
    }

    // Update metapage if needed
    if (info == XLOG_BTREE_UNLINK_PAGE_META) {
        _bt_restore_meta(record, 4);
    }
}
```