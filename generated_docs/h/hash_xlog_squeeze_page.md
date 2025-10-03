# hash_xlog_squeeze_page

## Location
[src/backend/access/hash/hash_xlog.c:627-860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash_xlog.c#L627-L860)

## Overview
Replays a hash index squeeze operation during WAL recovery, consolidating overflow pages and updating multiple related pages including bucket, overflow, bitmap, and meta pages.

## Definition
static void hash_xlog_squeeze_page(XLogReaderState *record)

## Detailed Description
This function handles the replay of a complete hash index squeeze operation during PostgreSQL's crash recovery process. A squeeze operation removes an overflow page by moving its contents to other pages and updating all related page linkages. The operation is complex and involves multiple buffers: primary bucket page (for locking), write buffer (destination for moved tuples), overflow buffer (page being freed), previous buffer (page before the freed page), next buffer (page after the freed page), bitmap buffer (tracks free pages), and meta buffer (index metadata). The function ensures proper locking order, moves tuples when necessary, initializes the freed page as unused, updates page linkages, marks the page as free in the bitmap, and updates the metadata to track the newly available overflow page.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the squeeze operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogReadBufferForRedoExtended](../X/XLogReadBufferForRedoExtended.md)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - XLogRecHasBlockRef
  - [BufferGetPage](../B/BufferGetPage.md)
  - [BufferGetPageSize](../B/BufferGetPageSize.md)
  - IndexTupleSize
  - PageAddItem
  - HashPageGetOpaque
  - HashPageGetBitmap
  - HashPageGetMeta
  - [_hash_pageinit](_hash_pageinit.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [BufferIsValid](../B/BufferIsValid.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - CLRBIT
  - [xl_hash_squeeze_page](../x/xl_hash_squeeze_page.md)
  - XLogRedoAction
  - HashPageOpaque
  - HashMetaPage
  - RBM_NORMAL
  - BLK_NEEDS_REDO
  - BLK_NOTFOUND
  - InvalidOffsetNumber
  - InvalidBucket
  - LH_UNUSED_PAGE
  - HASHO_PAGE_ID
  - Item
- Called from (representative examples):
  - [hash_redo](hash_redo.md)

## Notes and Other Information
- This is a static function used only within the hash WAL recovery module
- Most complex of the hash WAL recovery functions, handling up to 7 different buffers
- Implements careful locking protocol to prevent concurrent scan issues during replay
- Updates multiple data structures: page contents, page linkages, free space bitmap, and metadata
- Handles conditional logic based on whether pages are the same (optimization for common cases)
- Part of PostgreSQL's hash index space reclamation WAL recovery infrastructure
- Includes extensive assertion checks and error handling for data integrity
- Buffer release order is carefully managed to maintain proper lock hierarchy
- During replay, bitmap and meta page updates don't require holding locks on other pages since no concurrent updates can occur

## Simplified Source

```c
static void hash_xlog_squeeze_page(XLogReaderState *record) {
    XLogRecPtr lsn = record->EndRecPtr;
    xl_hash_squeeze_page *xldata = (xl_hash_squeeze_page *) XLogRecGetData(record);
    Buffer bucketbuf = InvalidBuffer, writebuf = InvalidBuffer, ovflbuf;
    Buffer prevbuf = InvalidBuffer, mapbuf;
    XLogRedoAction action;

    // Get cleanup lock on primary bucket page to prevent concurrent scans
    if (xldata->is_prim_bucket_same_wrt)
        action = XLogReadBufferForRedoExtended(record, 1, RBM_NORMAL, true, &writebuf);
    else {
        (void) XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, true, &bucketbuf);

        if (xldata->ntups > 0 || xldata->is_prev_bucket_same_wrt)
            action = XLogReadBufferForRedo(record, 1, &writebuf);
        else
            action = BLK_NOTFOUND;
    }

    // Replay adding tuples to write buffer (destination page)
    if (action == BLK_NEEDS_REDO) {
        Page writepage = BufferGetPage(writebuf);
        char *data = XLogRecGetBlockData(record, 1, NULL);
        uint16 ninserted = 0;
        bool mod_wbuf = false;

        if (xldata->ntups > 0) {
            OffsetNumber *towrite = (OffsetNumber *) data;
            data += sizeof(OffsetNumber) * xldata->ntups;

            // Insert each tuple from the squeezed page
            while (ninserted < xldata->ntups) {
                IndexTuple itup = (IndexTuple) data;
                Size itemsz = MAXALIGN(IndexTupleSize(itup));

                OffsetNumber l = PageAddItem(writepage, (Item) itup, itemsz,
                                           towrite[ninserted], false, false);
                if (l == InvalidOffsetNumber)
                    elog(ERROR, "hash_xlog_squeeze_page: failed to add item");

                data += itemsz;
                ninserted++;
            }
            mod_wbuf = true;
        }

        // Update next block pointer if this is the previous page
        if (xldata->is_prev_bucket_same_wrt) {
            HashPageOpaque writeopaque = HashPageGetOpaque(writepage);
            writeopaque->hasho_nextblkno = xldata->nextblkno;
            mod_wbuf = true;
        }

        if (mod_wbuf) {
            PageSetLSN(writepage, lsn);
            MarkBufferDirty(writebuf);
        }
    }

    // Initialize the freed overflow page as unused
    if (XLogReadBufferForRedo(record, 2, &ovflbuf) == BLK_NEEDS_REDO) {
        Page ovflpage = BufferGetPage(ovflbuf);
        HashPageOpaque ovflopaque;

        _hash_pageinit(ovflpage, BufferGetPageSize(ovflbuf));
        ovflopaque = HashPageGetOpaque(ovflpage);

        ovflopaque->hasho_prevblkno = InvalidBlockNumber;
        ovflopaque->hasho_nextblkno = InvalidBlockNumber;
        ovflopaque->hasho_bucket = InvalidBucket;
        ovflopaque->hasho_flag = LH_UNUSED_PAGE;
        ovflopaque->hasho_page_id = HASHO_PAGE_ID;

        PageSetLSN(ovflpage, lsn);
        MarkBufferDirty(ovflbuf);
    }
    if (BufferIsValid(ovflbuf)) UnlockReleaseBuffer(ovflbuf);

    // Update previous page to skip the freed page
    if (!xldata->is_prev_bucket_same_wrt &&
        XLogReadBufferForRedo(record, 3, &prevbuf) == BLK_NEEDS_REDO) {
        Page prevpage = BufferGetPage(prevbuf);
        HashPageOpaque prevopaque = HashPageGetOpaque(prevpage);

        prevopaque->hasho_nextblkno = xldata->nextblkno;
        PageSetLSN(prevpage, lsn);
        MarkBufferDirty(prevbuf);
    }
    if (BufferIsValid(prevbuf)) UnlockReleaseBuffer(prevbuf);

    // Update next page to skip the freed page
    if (XLogRecHasBlockRef(record, 4)) {
        Buffer nextbuf;
        if (XLogReadBufferForRedo(record, 4, &nextbuf) == BLK_NEEDS_REDO) {
            Page nextpage = BufferGetPage(nextbuf);
            HashPageOpaque nextopaque = HashPageGetOpaque(nextpage);

            nextopaque->hasho_prevblkno = xldata->prevblkno;
            PageSetLSN(nextpage, lsn);
            MarkBufferDirty(nextbuf);
        }
        if (BufferIsValid(nextbuf)) UnlockReleaseBuffer(nextbuf);
    }

    // Clean up bucket and write buffers
    if (BufferIsValid(writebuf)) UnlockReleaseBuffer(writebuf);
    if (BufferIsValid(bucketbuf)) UnlockReleaseBuffer(bucketbuf);

    // Update bitmap to mark page as free
    if (XLogReadBufferForRedo(record, 5, &mapbuf) == BLK_NEEDS_REDO) {
        Page mappage = BufferGetPage(mapbuf);
        uint32 *freep = HashPageGetBitmap(mappage);
        uint32 *bitmap_page_bit = (uint32 *) XLogRecGetBlockData(record, 5, NULL);

        CLRBIT(freep, *bitmap_page_bit);
        PageSetLSN(mappage, lsn);
        MarkBufferDirty(mapbuf);
    }
    if (BufferIsValid(mapbuf)) UnlockReleaseBuffer(mapbuf);

    // Update metapage with new first free page
    if (XLogRecHasBlockRef(record, 6)) {
        Buffer metabuf;
        if (XLogReadBufferForRedo(record, 6, &metabuf) == BLK_NEEDS_REDO) {
            uint32 *firstfree_ovflpage = (uint32 *) XLogRecGetBlockData(record, 6, NULL);
            Page page = BufferGetPage(metabuf);
            HashMetaPage metap = HashPageGetMeta(page);

            metap->hashm_firstfree = *firstfree_ovflpage;
            PageSetLSN(page, lsn);
            MarkBufferDirty(metabuf);
        }
        if (BufferIsValid(metabuf)) UnlockReleaseBuffer(metabuf);
    }
}
```