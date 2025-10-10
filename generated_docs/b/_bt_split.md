# _bt_split

## Location
[src/backend/access/nbtree/nbtinsert.c:1467-2098](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L1467-L2098)

## Overview
_bt_split splits a B-tree page when insufficient space exists for a new tuple, creating a new right sibling page and redistributing tuples between the original (left) and new (right) pages.

## Definition

```c
enumber,
				rightpagenumber;
```
## Detailed Description
This function performs the complex operation of splitting a B-tree page into two pages. The process involves several critical steps:

1. **Split Point Selection**: Uses _bt_findsplitloc() to determine the optimal point to split the page, balancing the distribution of tuples between left and right pages.

2. **Page Setup**: Creates a temporary left page and allocates a new right page buffer. The original page becomes the left page, and tuples are redistributed accordingly.

3. **High Key Management**: 
   - For leaf pages: Creates a truncated high key using suffix truncation when possible
   - For internal pages: Uses the first right tuple directly as the high key to maintain separator key integrity

4. **Tuple Distribution**: Iterates through all tuples and distributes them to appropriate pages based on the split point, handling special cases like posting list splits.

5. **Sibling Link Updates**: Updates prev/next pointers to maintain the doubly-linked list structure of pages at the same level.

6. **WAL Logging**: Records all changes in a comprehensive WAL record for crash recovery, including specialized handling for posting list splits.

The function ensures atomicity through critical sections and handles complex scenarios like concurrent posting list splits.

## Parameters / Member Variables
- : The B-tree index relation being split
- : The heap relation referenced by the index
- : BTScanInsert structure used for suffix truncation on leaf pages (NULL for internal pages)
- : Buffer containing the page to be split (pinned and write-locked)
- : Left-sibling buffer when splitting non-leaf page (used to clear INCOMPLETE_SPLIT flag)
- : Offset where the new item should be inserted
- : Size of the new item being inserted
- : The new IndexTuple to be inserted
- : Original new item when posting list split is involved
- : New posting list tuple when posting list split is involved  
- : Offset within posting list for posting list splits (0 if not applicable)

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_findsplitloc](_bt_findsplitloc.md) (to determine split point)
  - [_bt_allocbuf](_bt_allocbuf.md) (to allocate new right page)
  - [_bt_truncate](_bt_truncate.md) (for suffix truncation on leaf pages)
  - [_bt_pgaddtup](_bt_pgaddtup.md) (to add tuples to pages)
  - [PageGetTempPage](../P/PageGetTempPage.md), PageRestoreTempPage (for temporary page management)
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterBuffer, XLogInsert (for WAL logging)
  - Various page and buffer management functions
- Called from (representative examples):
  - [_bt_insertonpg](_bt_insertonpg.md) (when page split is needed during insertion)

## Notes and Other Information
- Returns the new right sibling buffer, pinned and write-locked
- The original buffer (left page) remains pinned and write-locked
- Uses critical sections to ensure atomicity of the split operation
- Handles both leaf and internal page splits with different logic for high key creation
- Supports concurrent posting list splits through special parameter handling
- Updates sibling page links and handles INCOMPLETE_SPLIT flag clearing
- Includes extensive error handling with proper cleanup of allocated resources
- The function is static and only used within the nbtinsert.c module
- Maintains B-tree invariants including page ordering and key distribution

## Simplified Source

```c
static Buffer
_bt_split(Relation rel, Relation heaprel, BTScanInsert itup_key, Buffer buf,
          Buffer cbuf, OffsetNumber newitemoff, Size newitemsz, IndexTuple newitem,
          IndexTuple orignewitem, IndexTuple nposting, uint16 postingoff)
{
    Page origpage = BufferGetPage(buf);
    BTPageOpaque oopaque = BTPageGetOpaque(origpage);
    bool isleaf = P_ISLEAF(oopaque);
    bool isrightmost = P_RIGHTMOST(oopaque);
    OffsetNumber maxoff = PageGetMaxOffsetNumber(origpage);
    BlockNumber origpagenumber = BufferGetBlockNumber(buf);

    // Choose split point using findsplitloc
    bool newitemonleft;
    OffsetNumber firstrightoff = _bt_findsplitloc(rel, origpage, newitemoff,
                                                 newitemsz, newitem, &newitemonleft);

    // Create temporary left page
    Page leftpage = PageGetTempPage(origpage);
    _bt_pageinit(leftpage, BufferGetPageSize(buf));
    BTPageOpaque lopaque = BTPageGetOpaque(leftpage);

    // Set up left page properties
    lopaque->btpo_flags = oopaque->btpo_flags;
    lopaque->btpo_flags &= ~(BTP_ROOT | BTP_SPLIT_END | BTP_HAS_GARBAGE);
    lopaque->btpo_flags |= BTP_INCOMPLETE_SPLIT;
    lopaque->btpo_prev = oopaque->btpo_prev;
    lopaque->btpo_level = oopaque->btpo_level;
    PageSetLSN(leftpage, PageGetLSN(origpage));

    // Determine firstright tuple and create high key
    IndexTuple firstright, lefthighkey;
    Size itemsz;

    if (!newitemonleft && newitemoff == firstrightoff) {
        firstright = newitem;
        itemsz = newitemsz;
    } else {
        ItemId itemid = PageGetItemId(origpage, firstrightoff);
        firstright = (IndexTuple) PageGetItem(origpage, itemid);
        itemsz = ItemIdGetLength(itemid);
    }

    // Create high key for left page
    if (isleaf) {
        // For leaf pages, attempt suffix truncation
        IndexTuple lastleft;
        if (newitemonleft && newitemoff == firstrightoff) {
            lastleft = newitem;
        } else {
            OffsetNumber lastleftoff = OffsetNumberPrev(firstrightoff);
            ItemId itemid = PageGetItemId(origpage, lastleftoff);
            lastleft = (IndexTuple) PageGetItem(origpage, itemid);
        }
        lefthighkey = _bt_truncate(rel, lastleft, firstright, itup_key);
    } else {
        // For internal pages, use firstright directly (no truncation)
        lefthighkey = firstright;
    }

    // Add high key to left page
    if (PageAddItem(leftpage, (Item) lefthighkey, MAXALIGN(IndexTupleSize(lefthighkey)),
                    P_HIKEY, false, false) == InvalidOffsetNumber)
        elog(ERROR, "failed to add high key to left sibling");

    // Allocate new right page
    Buffer rbuf = _bt_allocbuf(rel, heaprel);
    Page rightpage = BufferGetPage(rbuf);
    BlockNumber rightpagenumber = BufferGetBlockNumber(rbuf);
    BTPageOpaque ropaque = BTPageGetOpaque(rightpage);

    // Set up right page properties
    ropaque->btpo_flags = oopaque->btpo_flags;
    ropaque->btpo_flags &= ~(BTP_ROOT | BTP_SPLIT_END | BTP_HAS_GARBAGE);
    ropaque->btpo_prev = origpagenumber;
    ropaque->btpo_next = oopaque->btpo_next;
    ropaque->btpo_level = oopaque->btpo_level;
    ropaque->btpo_cycleid = _bt_vacuum_cycleid(rel);

    // Update left page next pointer
    lopaque->btpo_next = rightpagenumber;
    lopaque->btpo_cycleid = ropaque->btpo_cycleid;

    // Add high key to right page if not rightmost
    OffsetNumber afterrightoff = P_HIKEY;
    if (!isrightmost) {
        ItemId itemid = PageGetItemId(origpage, P_HIKEY);
        IndexTuple righthighkey = (IndexTuple) PageGetItem(origpage, itemid);
        if (PageAddItem(rightpage, (Item) righthighkey, ItemIdGetLength(itemid),
                        afterrightoff, false, false) == InvalidOffsetNumber) {
            memset(rightpage, 0, BufferGetPageSize(rbuf));
            elog(ERROR, "failed to add high key to right sibling");
        }
        afterrightoff = OffsetNumberNext(afterrightoff);
    }

    // Distribute tuples between left and right pages
    OffsetNumber afterleftoff = OffsetNumberNext(P_HIKEY);
    OffsetNumber minusinfoff = (!isleaf) ? afterrightoff : InvalidOffsetNumber;

    for (OffsetNumber i = P_FIRSTDATAKEY(oopaque); i <= maxoff; i = OffsetNumberNext(i)) {
        ItemId itemid = PageGetItemId(origpage, i);
        IndexTuple dataitem = (IndexTuple) PageGetItem(origpage, itemid);
        Size itemsz = ItemIdGetLength(itemid);

        // Handle posting list replacement if needed
        if (postingoff != 0 && i == OffsetNumberPrev(newitemoff))
            dataitem = nposting;

        // Insert newitem if this is its position
        if (i == newitemoff) {
            if (newitemonleft) {
                _bt_pgaddtup(leftpage, newitemsz, newitem, afterleftoff, false);
                afterleftoff = OffsetNumberNext(afterleftoff);
            } else {
                _bt_pgaddtup(rightpage, newitemsz, newitem, afterrightoff,
                            afterrightoff == minusinfoff);
                afterrightoff = OffsetNumberNext(afterrightoff);
            }
        }

        // Distribute existing tuple
        if (i < firstrightoff) {
            _bt_pgaddtup(leftpage, itemsz, dataitem, afterleftoff, false);
            afterleftoff = OffsetNumberNext(afterleftoff);
        } else {
            _bt_pgaddtup(rightpage, itemsz, dataitem, afterrightoff,
                        afterrightoff == minusinfoff);
            afterrightoff = OffsetNumberNext(afterrightoff);
        }
    }

    // Handle newitem at end of page
    if (newitemoff > maxoff) {
        _bt_pgaddtup(rightpage, newitemsz, newitem, afterrightoff,
                    afterrightoff == minusinfoff);
    }

    // Update sibling links
    Buffer sbuf = InvalidBuffer;
    if (!isrightmost) {
        sbuf = _bt_getbuf(rel, oopaque->btpo_next, BT_WRITE);
        Page spage = BufferGetPage(sbuf);
        BTPageOpaque sopaque = BTPageGetOpaque(spage);

        if (sopaque->btpo_prev != origpagenumber) {
            memset(rightpage, 0, BufferGetPageSize(rbuf));
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                   errmsg_internal("right sibling's left-link doesn't match")));
        }

        // Set SPLIT_END flag if appropriate
        if (sopaque->btpo_cycleid != ropaque->btpo_cycleid)
            ropaque->btpo_flags |= BTP_SPLIT_END;
    }

    // Apply changes atomically
    START_CRIT_SECTION();

    // Copy left page back to original buffer
    PageRestoreTempPage(leftpage, origpage);
    MarkBufferDirty(buf);
    MarkBufferDirty(rbuf);

    // Update sibling pointer
    if (!isrightmost) {
        BTPageOpaque sopaque = BTPageGetOpaque(BufferGetPage(sbuf));
        sopaque->btpo_prev = rightpagenumber;
        MarkBufferDirty(sbuf);
    }

    // Clear INCOMPLETE_SPLIT on child if needed
    if (!isleaf) {
        Page cpage = BufferGetPage(cbuf);
        BTPageOpaque cpageop = BTPageGetOpaque(cpage);
        cpageop->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
        MarkBufferDirty(cbuf);
    }

    // WAL logging
    if (RelationNeedsWAL(rel)) {
        xl_btree_split xlrec;
        xlrec.level = ropaque->btpo_level;
        xlrec.firstrightoff = firstrightoff;
        xlrec.newitemoff = newitemoff;
        xlrec.postingoff = (postingoff != 0) ? postingoff : 0;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfBtreeSplit);
        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
        XLogRegisterBuffer(1, rbuf, REGBUF_WILL_INIT);

        if (!isrightmost)
            XLogRegisterBuffer(2, sbuf, REGBUF_STANDARD);
        if (!isleaf)
            XLogRegisterBuffer(3, cbuf, REGBUF_STANDARD);

        // Log items as needed
        if (newitemonleft && xlrec.postingoff == 0)
            XLogRegisterBufData(0, (char *) newitem, newitemsz);
        else if (xlrec.postingoff != 0)
            XLogRegisterBufData(0, (char *) orignewitem, newitemsz);

        XLogRegisterBufData(0, (char *) lefthighkey, MAXALIGN(IndexTupleSize(lefthighkey)));
        XLogRegisterBufData(1, (char *) rightpage + ((PageHeader) rightpage)->pd_upper,
                           ((PageHeader) rightpage)->pd_special - ((PageHeader) rightpage)->pd_upper);

        uint8 xlinfo = newitemonleft ? XLOG_BTREE_SPLIT_L : XLOG_BTREE_SPLIT_R;
        XLogRecPtr recptr = XLogInsert(RM_BTREE_ID, xlinfo);

        PageSetLSN(origpage, recptr);
        PageSetLSN(rightpage, recptr);
        if (!isrightmost)
            PageSetLSN(BufferGetPage(sbuf), recptr);
        if (!isleaf)
            PageSetLSN(BufferGetPage(cbuf), recptr);
    }

    END_CRIT_SECTION();

    // Clean up
    if (!isrightmost)
        _bt_relbuf(rel, sbuf);
    if (!isleaf)
        _bt_relbuf(rel, cbuf);
    if (isleaf)
        pfree(lefthighkey);

    return rbuf;
}
```