# hash_xlog_add_ovfl_page

## Location
[src/backend/access/hash/hash_xlog.c:173-310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash_xlog.c#L173-L310)

## Overview
Replays the addition of an overflow page to a hash index during WAL recovery, managing the complex process of linking pages and updating bitmap and metapage structures.

## Definition

```c
static void
hash_xlog_add_ovfl_page(XLogReaderState *record)
```
## Detailed Description
This function handles WAL replay for adding overflow pages to hash indexes when bucket pages become full. Hash indexes use overflow pages to store additional tuples when the primary bucket page cannot accommodate more data. This operation involves multiple components: creating the new overflow page, linking it to the existing page chain, updating bitmap pages to mark the page as allocated, potentially creating new bitmap pages if needed, and updating metapage statistics.

The function operates on up to 5 different buffers: the new overflow page (block 0), the left page that will point to it (block 1), an existing bitmap page (block 2), a potential new bitmap page (block 3), and the metapage (block 4). The function maintains proper page linkage by setting forward and backward pointers, updates bitmap allocation status, and manages metapage statistics including the first free overflow page pointer and overflow point counters.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record with overflow page data including bucket number, bitmap size (bmsize), and bitmap page found status (bmpage_found)

## Dependencies
- Functions called/Symbols referenced:
  - [xl_hash_add_ovfl_page](../x/xl_hash_add_ovfl_page.md) (WAL record structure)
  - XLogRecGetData (extracts record data)
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md) (gets block information)
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md) (initializes buffer for redo)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md) (gets block data)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md) (reads buffer for redo)
  - XLogRecHasBlockRef (checks if record has block reference)
  - [_hash_initbuf](_hash_initbuf.md) (initializes hash page buffer)
  - [_hash_initbitmapbuffer](_hash_initbitmapbuffer.md) (initializes bitmap buffer)
  - HashPageGetOpaque (gets page opaque data)
  - HashPageGetBitmap (gets bitmap from page)
  - HashPageGetMeta (gets metapage metadata)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (gets buffer block number)
  - BLK_NEEDS_REDO (indicates block needs redo)
  - LH_OVERFLOW_PAGE (overflow page type)
  - SETBIT (sets bit in bitmap)
  - BlockNumberIsValid (validates block number)
- Called from:
  - [hash_redo](hash_redo.md) (main hash WAL replay function)

## Notes and Other Information
- This is a static function used only within the hash WAL recovery subsystem
- The function manages complex page linkage by updating hasho_prevblkno and hasho_nextblkno pointers
- During normal operation, all pages would be locked simultaneously, but during replay concurrent access isn't possible
- The function may create new bitmap pages if existing ones are full, updating the metapage's bitmap registry
- Metapage updates include managing hashm_firstfree, hashm_spares, hashm_mapp, and hashm_nmaps fields
- The function handles conditional operations based on whether bitmap pages were found during the original operation
- Bitmap allocation tracking ensures proper overflow page management and prevents page leaks

## Simplified Source

```c
static void hash_xlog_add_ovfl_page(XLogReaderState *record) {
    XLogRecPtr lsn = record->EndRecPtr;
    xl_hash_add_ovfl_page *xlrec = (xl_hash_add_ovfl_page *) XLogRecGetData(record);
    Buffer leftbuf, ovflbuf, metabuf;
    BlockNumber leftblk, rightblk, newmapblk = InvalidBlockNumber;
    bool new_bmpage = false;

    // Get block numbers for overflow and left pages
    XLogRecGetBlockTag(record, 0, NULL, NULL, &rightblk);
    XLogRecGetBlockTag(record, 1, NULL, NULL, &leftblk);

    // Initialize the new overflow page
    ovflbuf = XLogInitBufferForRedo(record, 0);
    char *data = XLogRecGetBlockData(record, 0, NULL);
    uint32 *num_bucket = (uint32 *) data;

    _hash_initbuf(ovflbuf, InvalidBlockNumber, *num_bucket, LH_OVERFLOW_PAGE, true);

    // Set backlink to left page
    Page ovflpage = BufferGetPage(ovflbuf);
    HashPageOpaque ovflopaque = HashPageGetOpaque(ovflpage);
    ovflopaque->hasho_prevblkno = leftblk;

    PageSetLSN(ovflpage, lsn);
    MarkBufferDirty(ovflbuf);

    // Update left page to point to new overflow page
    if (XLogReadBufferForRedo(record, 1, &leftbuf) == BLK_NEEDS_REDO) {
        Page leftpage = BufferGetPage(leftbuf);
        HashPageOpaque leftopaque = HashPageGetOpaque(leftpage);
        leftopaque->hasho_nextblkno = rightblk;

        PageSetLSN(leftpage, lsn);
        MarkBufferDirty(leftbuf);
    }

    if (BufferIsValid(leftbuf)) UnlockReleaseBuffer(leftbuf);
    UnlockReleaseBuffer(ovflbuf);

    // Update existing bitmap page if referenced
    if (XLogRecHasBlockRef(record, 2)) {
        Buffer mapbuffer;
        if (XLogReadBufferForRedo(record, 2, &mapbuffer) == BLK_NEEDS_REDO) {
            Page mappage = BufferGetPage(mapbuffer);
            uint32 *freep = HashPageGetBitmap(mappage);
            uint32 *bitmap_page_bit = (uint32 *) XLogRecGetBlockData(record, 2, NULL);

            SETBIT(freep, *bitmap_page_bit);
            PageSetLSN(mappage, lsn);
            MarkBufferDirty(mapbuffer);
        }
        if (BufferIsValid(mapbuffer)) UnlockReleaseBuffer(mapbuffer);
    }

    // Initialize new bitmap page if needed
    if (XLogRecHasBlockRef(record, 3)) {
        Buffer newmapbuf = XLogInitBufferForRedo(record, 3);
        _hash_initbitmapbuffer(newmapbuf, xlrec->bmsize, true);

        new_bmpage = true;
        newmapblk = BufferGetBlockNumber(newmapbuf);

        MarkBufferDirty(newmapbuf);
        PageSetLSN(BufferGetPage(newmapbuf), lsn);
        UnlockReleaseBuffer(newmapbuf);
    }

    // Update metapage with new overflow page information
    if (XLogReadBufferForRedo(record, 4, &metabuf) == BLK_NEEDS_REDO) {
        uint32 *firstfree_ovflpage = (uint32 *) XLogRecGetBlockData(record, 4, NULL);
        Page page = BufferGetPage(metabuf);
        HashMetaPage metap = HashPageGetMeta(page);

        metap->hashm_firstfree = *firstfree_ovflpage;

        if (!xlrec->bmpage_found) {
            metap->hashm_spares[metap->hashm_ovflpoint]++;

            if (new_bmpage) {
                metap->hashm_mapp[metap->hashm_nmaps] = newmapblk;
                metap->hashm_nmaps++;
                metap->hashm_spares[metap->hashm_ovflpoint]++;
            }
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(metabuf);
    }

    if (BufferIsValid(metabuf)) UnlockReleaseBuffer(metabuf);
}
```