# btree_xlog_split

## Location
[src/backend/access/nbtree/nbtxlog.c:251-463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtxlog.c#L251-L463)

## Overview
Handles B-tree page split WAL record replay during recovery, reconstructing both left and right pages from the split operation.

## Definition
```c
static void btree_xlog_split(bool newitemonleft, XLogReaderState *record)
```

## Detailed Description
This function is one of the most complex WAL replay operations in the B-tree implementation. It reconstructs a complete B-tree page split operation from WAL record data. A page split occurs when a B-tree page becomes too full and needs to be divided into two pages: the original (left) page and a new (right) page.

The function handles multiple complex scenarios:
1. **Basic page split**: Dividing tuples between left and right pages
2. **Posting list splits**: When the split involves compressed posting lists
3. **Chain link management**: Properly updating prev/next pointers between pages
4. **Incomplete split handling**: Managing the BTP_INCOMPLETE_SPLIT flag
5. **New item insertion**: Inserting the triggering item during split replay

The process involves reconstructing the right page from scratch using _bt_restore_page, then carefully reconstructing the left page by creating a temporary page and adding items in the correct order to maintain physical tuple ordering for WAL consistency checking.

For internal page splits, it also clears incomplete split flags on child pages, maintaining tree consistency across levels.

## Parameters / Member Variables
- `newitemonleft`: Boolean indicating whether the new item that triggered the split should be placed on the left page
- `record`: XLogReaderState containing the complete WAL record data for the split operation

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_clear_incomplete_split](_bt_clear_incomplete_split.md)
  - XLogRecGetData
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md)
  - [XLogRecGetBlockTagExtended](../X/XLogRecGetBlockTagExtended.md)
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - [_bt_pageinit](_bt_pageinit.md)
  - [_bt_restore_page](_bt_restore_page.md)
  - BTPageGetOpaque
  - [PageGetTempPageCopySpecial](../P/PageGetTempPageCopySpecial.md)
  - PageAddItem
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - ItemIdGetLength
  - OffsetNumberPrev
  - OffsetNumberNext
  - [CopyIndexTuple](../C/CopyIndexTuple.md)
  - [_bt_swap_posting](_bt_swap_posting.md)
  - [PageRestoreTempPage](../P/PageRestoreTempPage.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Data types used:
  - [xl_btree_split](../x/xl_btree_split.md)
  - BTPageOpaque
  - ItemId
  - [IndexTuple](../I/IndexTuple.md)
- Constants used:
  - BLK_NEEDS_REDO
  - P_NONE
  - BTP_LEAF
  - BTP_INCOMPLETE_SPLIT
  - P_HIKEY
  - P_FIRSTDATAKEY
  - InvalidOffsetNumber
- Called from (representative examples):
  - [btree_redo](btree_redo.md) (for both leaf and internal page splits)

## Notes and Other Information
- This is a static function used internally within nbtxlog.c for B-tree WAL recovery
- One of the most complex WAL replay operations due to the intricate nature of B-tree splits
- Handles both leaf and internal page splits with appropriate flag management
- Supports posting list splits for compressed leaf page entries
- Maintains physical tuple ordering for WAL consistency checking by using temporary pages
- Carefully manages buffer release order to prevent readers from observing inconsistent states
- Sets BTP_INCOMPLETE_SPLIT flag on the left page until the parent gets the downlink
- Updates sibling page links (prev/next pointers) to maintain B-tree chain integrity
- Includes comprehensive error handling with detailed error messages for debugging
- The function mirrors the logic of the original _bt_split() function during replay
- Critical for maintaining B-tree consistency during crash recovery scenarios

## Simplified Source

```c
static void btree_xlog_split(bool newitemonleft, XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_btree_split *xlrec = (xl_btree_split *) XLogRecGetData(record);
    bool isleaf = (xlrec->level == 0);
    Buffer buf, rbuf;
    Page rpage;
    BTPageOpaque ropaque;
    char *datapos;
    Size datalen;
    BlockNumber origpagenumber, rightpagenumber, spagenumber;

    // Get block numbers for pages involved in split
    XLogRecGetBlockTag(record, 0, NULL, NULL, &origpagenumber);
    XLogRecGetBlockTag(record, 1, NULL, NULL, &rightpagenumber);
    if (!XLogRecGetBlockTagExtended(record, 2, NULL, NULL, &spagenumber, NULL))
        spagenumber = P_NONE;

    // For internal pages, clear incomplete split flag on child page
    if (!isleaf)
        _bt_clear_incomplete_split(record, 3);

    // Reconstruct right (new) sibling page from scratch
    rbuf = XLogInitBufferForRedo(record, 1);
    datapos = XLogRecGetBlockData(record, 1, &datalen);
    rpage = (Page) BufferGetPage(rbuf);

    // Initialize right page and set its opaque data
    _bt_pageinit(rpage, BufferGetPageSize(rbuf));
    ropaque = BTPageGetOpaque(rpage);
    ropaque->btpo_prev = origpagenumber;
    ropaque->btpo_next = spagenumber;
    ropaque->btpo_level = xlrec->level;
    ropaque->btpo_flags = isleaf ? BTP_LEAF : 0;
    ropaque->btpo_cycleid = 0;

    // Restore right page content from WAL data
    _bt_restore_page(rpage, datapos, datalen);
    PageSetLSN(rpage, lsn);
    MarkBufferDirty(rbuf);

    // Reconstruct original page (left half of split)
    if (XLogReadBufferForRedo(record, 0, &buf) == BLK_NEEDS_REDO)
    {
        Page origpage = (Page) BufferGetPage(buf);
        BTPageOpaque oopaque = BTPageGetOpaque(origpage);
        OffsetNumber off;
        IndexTuple newitem = NULL, left_hikey = NULL, nposting = NULL;
        Size newitemsz = 0, left_hikeysz = 0;
        Page leftpage;
        OffsetNumber leftoff, replacepostingoff = InvalidOffsetNumber;

        datapos = XLogRecGetBlockData(record, 0, &datalen);

        // Handle new item and posting list splits
        if (newitemonleft || xlrec->postingoff != 0)
        {
            newitem = (IndexTuple) datapos;
            newitemsz = MAXALIGN(IndexTupleSize(newitem));
            datapos += newitemsz;
            datalen -= newitemsz;

            // Handle posting list split if needed
            if (xlrec->postingoff != 0)
            {
                ItemId itemid;
                IndexTuple oposting;

                replacepostingoff = OffsetNumberPrev(xlrec->newitemoff);
                newitem = CopyIndexTuple(newitem);
                itemid = PageGetItemId(origpage, replacepostingoff);
                oposting = (IndexTuple) PageGetItem(origpage, itemid);
                nposting = _bt_swap_posting(newitem, oposting, xlrec->postingoff);
            }
        }

        // Extract left page high key
        left_hikey = (IndexTuple) datapos;
        left_hikeysz = MAXALIGN(IndexTupleSize(left_hikey));
        datapos += left_hikeysz;
        datalen -= left_hikeysz;

        // Create temporary page to rebuild left page in correct order
        leftpage = PageGetTempPageCopySpecial(origpage);

        // Add high key
        leftoff = P_HIKEY;
        if (PageAddItem(leftpage, (Item) left_hikey, left_hikeysz, P_HIKEY,
                       false, false) == InvalidOffsetNumber)
            elog(ERROR, "failed to add high key to left page after split");
        leftoff = OffsetNumberNext(leftoff);

        // Add existing items, new item, and posting replacements as needed
        for (off = P_FIRSTDATAKEY(oopaque); off < xlrec->firstrightoff; off++)
        {
            ItemId itemid;
            Size itemsz;
            IndexTuple item;

            // Handle posting list replacement
            if (off == replacepostingoff)
            {
                if (PageAddItem(leftpage, (Item) nposting,
                               MAXALIGN(IndexTupleSize(nposting)), leftoff,
                               false, false) == InvalidOffsetNumber)
                    elog(ERROR, "failed to add new posting list item to left page after split");
                leftoff = OffsetNumberNext(leftoff);
                continue;
            }
            // Handle new item insertion
            else if (newitemonleft && off == xlrec->newitemoff)
            {
                if (PageAddItem(leftpage, (Item) newitem, newitemsz, leftoff,
                               false, false) == InvalidOffsetNumber)
                    elog(ERROR, "failed to add new item to left page after split");
                leftoff = OffsetNumberNext(leftoff);
            }

            // Add existing item
            itemid = PageGetItemId(origpage, off);
            itemsz = ItemIdGetLength(itemid);
            item = (IndexTuple) PageGetItem(origpage, itemid);
            if (PageAddItem(leftpage, (Item) item, itemsz, leftoff,
                           false, false) == InvalidOffsetNumber)
                elog(ERROR, "failed to add old item to left page after split");
            leftoff = OffsetNumberNext(leftoff);
        }

        // Handle new item at end if needed
        if (newitemonleft && off == xlrec->newitemoff)
        {
            if (PageAddItem(leftpage, (Item) newitem, newitemsz, leftoff,
                           false, false) == InvalidOffsetNumber)
                elog(ERROR, "failed to add new item to left page after split");
        }

        // Replace original page with reconstructed left page
        PageRestoreTempPage(leftpage, origpage);

        // Set opaque fields for left page
        oopaque->btpo_flags = BTP_INCOMPLETE_SPLIT;
        if (isleaf)
            oopaque->btpo_flags |= BTP_LEAF;
        oopaque->btpo_next = rightpagenumber;
        oopaque->btpo_cycleid = 0;

        PageSetLSN(origpage, lsn);
        MarkBufferDirty(buf);
    }

    // Fix left-link of page to the right of new right sibling
    if (spagenumber != P_NONE)
    {
        Buffer sbuf;
        if (XLogReadBufferForRedo(record, 2, &sbuf) == BLK_NEEDS_REDO)
        {
            Page spage = (Page) BufferGetPage(sbuf);
            BTPageOpaque spageop = BTPageGetOpaque(spage);
            spageop->btpo_prev = rightpagenumber;
            PageSetLSN(spage, lsn);
            MarkBufferDirty(sbuf);
        }
        if (BufferIsValid(sbuf))
            UnlockReleaseBuffer(sbuf);
    }

    // Release buffers in proper order
    UnlockReleaseBuffer(rbuf);
    if (BufferIsValid(buf))
        UnlockReleaseBuffer(buf);
}
```