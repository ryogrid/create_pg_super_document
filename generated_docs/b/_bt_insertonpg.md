# _bt_insertonpg

## Location
[src/backend/access/nbtree/nbtinsert.c:1105-1466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L1105-L1466)

## Overview
_bt_insertonpg is a recursive function that performs tuple insertion on a specific page in a B-tree index, handling posting list splits, page splits, and parent insertions as needed.

## Definition

```c
static void
_bt_insertonpg(Relation rel,
			   Relation heaprel,
			   BTScanInsert itup_key,
			   Buffer buf,
			   Buffer cbuf,
			   BTStack stack,
			   IndexTuple itup,
			   Size itemsz,
			   OffsetNumber newitemoff,
			   int postingoff,
			   bool split_only_page)
```
## Detailed Description
This recursive procedure is the core insertion mechanism for B-tree indexes. It handles several complex scenarios:

1. **Posting List Splitting**: If postingoff != 0, it splits an existing posting list tuple that overlaps with the new tuple being inserted.

2. **Page Splitting**: When there's insufficient space on the target page, it calls _bt_split() to create a new page and distribute tuples between the old and new pages.

3. **Tuple Insertion**: Inserts the new tuple (which might be a result of posting list split) onto the appropriate page.

4. **Parent Management**: After a page split, it recursively calls _bt_insert_parent() to insert the appropriate child pointer in the parent page.

5. **Metadata Updates**: Updates the metapage when a root or fast root is split.

The function ensures WAL logging for crash recovery and maintains B-tree invariants throughout the insertion process. It operates with the assumption that the caller has already acquired the necessary buffer locks and handles buffer cleanup upon completion.

## Parameters / Member Variables
- `rel`: The B-tree index relation being modified
- `heaprel`: The heap relation that the index references
- `itup_key`: BTScanInsert structure containing search/insertion key information
- `buf`: Buffer containing the target page for insertion (must be pinned and write-locked)
- `cbuf`: Left-sibling buffer when inserting to non-leaf page (used to clear INCOMPLETE_SPLIT flag)
- `stack`: BTStack containing parent page information for potential recursive calls
- `itup`: The IndexTuple to be inserted
- `itemsz`: Size of the item being inserted (MAXALIGN'd size of itup)
- `newitemoff`: Offset number where the new item should be inserted
- `postingoff`: Offset within posting list for duplicate handling (0 if not splitting posting list)
- `split_only_page`: True if inserting because we split the only page on a tree level

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_split](_bt_split.md) (for page splitting)
  - [_bt_insert_parent](_bt_insert_parent.md) (for recursive parent insertion)
  - [_bt_swap_posting](_bt_swap_posting.md) (for posting list manipulation)
  - [PageGetFreeSpace](../P/PageGetFreeSpace.md) (to check available space)
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterData, XLogInsert (for WAL logging)
  - Various buffer and page management functions
- Called from (representative examples):
  - [_bt_doinsert](_bt_doinsert.md) (main insertion entry point)
  - [_bt_insert_parent](_bt_insert_parent.md) (recursive calls during split propagation)

## Notes and Other Information
- This is a static function within nbtinsert.c, not exposed to external modules
- The function assumes caller has completed any incomplete splits before calling
- Buffer locks are released upon function completion, regardless of success or failure
- Supports both leaf and internal page insertions with different handling logic
- Critical sections are used around page modifications to ensure atomicity
- The function includes extensive assertion checking for debugging and correctness validation
- Handles both simple insertions and complex scenarios involving posting list splits and page splits

## Simplified Source

```c
static void _bt_insertonpg(Relation rel, Relation heaprel, BTScanInsert itup_key,
                          Buffer buf, Buffer cbuf, BTStack stack, IndexTuple itup,
                          Size itemsz, OffsetNumber newitemoff, int postingoff,
                          bool split_only_page) {
    Page page;
    BTPageOpaque opaque;
    bool isleaf, isroot, isrightmost, isonly;
    IndexTuple oposting = NULL;
    IndexTuple nposting = NULL;

    page = BufferGetPage(buf);
    opaque = BTPageGetOpaque(page);
    isleaf = P_ISLEAF(opaque);
    isroot = P_ISROOT(opaque);
    isrightmost = P_RIGHTMOST(opaque);
    isonly = P_LEFTMOST(opaque) && P_RIGHTMOST(opaque);

    // Handle posting list split if needed
    if (postingoff != 0) {
        ItemId itemid = PageGetItemId(page, newitemoff);
        oposting = (IndexTuple) PageGetItem(page, itemid);

        // Create modified copy of itup and split posting list
        IndexTuple origitup = itup;
        itup = CopyIndexTuple(origitup);
        nposting = _bt_swap_posting(itup, oposting, postingoff);

        // Adjust offset for new item after posting list
        newitemoff = OffsetNumberNext(newitemoff);
    }

    // Check if page split is needed
    if (PageGetFreeSpace(page) < itemsz) {
        Buffer rbuf;

        // Split the page
        rbuf = _bt_split(rel, heaprel, itup_key, buf, cbuf, newitemoff, itemsz,
                        itup, origitup, nposting, postingoff);

        // Insert parent downlink to complete split
        _bt_insert_parent(rel, heaprel, buf, rbuf, stack, isroot, isonly);
    } else {
        // Direct insertion without split
        Buffer metabuf = InvalidBuffer;

        // Handle fast root updates if needed
        if (unlikely(split_only_page)) {
            metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
            // Update metapage fast root if appropriate
        }

        START_CRIT_SECTION();

        // Apply posting list split if needed
        if (postingoff != 0)
            memcpy(oposting, nposting, MAXALIGN(IndexTupleSize(nposting)));

        // Insert the tuple
        if (PageAddItem(page, (Item) itup, itemsz, newitemoff, false, false) == InvalidOffsetNumber)
            elog(PANIC, "failed to add new item to block %u in index \"%s\"",
                 BufferGetBlockNumber(buf), RelationGetRelationName(rel));

        MarkBufferDirty(buf);

        // Update metapage if needed
        if (BufferIsValid(metabuf)) {
            Page metapg = BufferGetPage(metabuf);
            BTMetaPageData *metad = BTPageGetMeta(metapg);
            metad->btm_fastroot = BufferGetBlockNumber(buf);
            metad->btm_fastlevel = opaque->btpo_level;
            MarkBufferDirty(metabuf);
        }

        // Clear INCOMPLETE_SPLIT flag on child if needed
        if (!isleaf) {
            Page cpage = BufferGetPage(cbuf);
            BTPageOpaque cpageop = BTPageGetOpaque(cpage);
            cpageop->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
            MarkBufferDirty(cbuf);
        }

        // WAL logging
        if (RelationNeedsWAL(rel)) {
            xl_btree_insert xlrec;
            xlrec.offnum = newitemoff;

            XLogBeginInsert();
            XLogRegisterData((char *) &xlrec, SizeOfBtreeInsert);
            XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
            XLogRegisterBufData(0, (char *) itup, IndexTupleSize(itup));

            XLogRecPtr recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_INSERT_LEAF);
            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();

        // Cleanup
        if (BufferIsValid(metabuf)) _bt_relbuf(rel, metabuf);
        if (!isleaf) _bt_relbuf(rel, cbuf);

        // Cache rightmost leaf page for fastpath optimization
        if (isrightmost && isleaf && !isroot) {
            BlockNumber blockcache = BufferGetBlockNumber(buf);
            if (_bt_getrootheight(rel) >= BTREE_FASTPATH_MIN_LEVEL)
                RelationSetTargetBlock(rel, blockcache);
        }

        _bt_relbuf(rel, buf);
    }

    // Cleanup posting split copies
    if (postingoff != 0) {
        pfree(nposting);
        pfree(itup);
    }
}
```