# _hash_freeovflpage

## Location
[src/backend/access/hash/hashovfl.c:490-776](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashovfl.c#L490-L776)

## Overview
Removes an overflow page from its bucket chain and marks it as free in the bitmap, while transferring any remaining tuples to a designated write page.

## Definition
```c
BlockNumber _hash_freeovflpage(Relation rel, Buffer bucketbuf, Buffer ovflbuf,
                              Buffer wbuf, IndexTuple *itups, OffsetNumber *itup_offsets,
                              Size *tups_size, uint16 nitups,
                              BufferAccessStrategy bstrategy)
```

## Detailed Description
This function performs the complete removal of an overflow page from a hash index bucket chain, which involves several critical operations:

1. **Chain maintenance**: Updates the doubly-linked list of overflow pages by fixing the previous and next page pointers
2. **Tuple migration**: Moves any remaining tuples from the overflow page to the designated write buffer
3. **Page cleanup**: Reinitializes the overflow page to an unused state with proper special space
4. **Bitmap management**: Clears the corresponding bit in the bitmap page and updates the firstfree pointer if necessary
5. **WAL logging**: Creates comprehensive WAL records to ensure atomicity during recovery

The function is designed to be called during VACUUM operations and bucket squeeze operations, using lock chaining to avoid deadlocks with concurrent operations.

## Parameters / Member Variables
- `rel`: The hash index relation being modified
- `bucketbuf`: Buffer for the primary bucket page
- `ovflbuf`: Buffer for the overflow page being freed (must be write-locked on entry)
- `wbuf`: Write buffer where tuples from the overflow page will be moved
- `itups`: Array of index tuples to be moved to the write buffer
- `itup_offsets`: Array of offset numbers for the tuples
- `tups_size`: Array of sizes for each tuple
- `nitups`: Number of tuples to move
- `bstrategy`: Buffer access strategy for controlling page fetches

## Dependencies
- Functions called/Symbols referenced:
  - [_hash_checkpage](_hash_checkpage.md) (page validation)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)/BufferGetPage (buffer access)
  - HashPageGetOpaque/HashPageGetMeta/HashPageGetBitmap (page structure access)
  - [_hash_getbuf_with_strategy](_hash_getbuf_with_strategy.md)/_hash_getbuf (buffer management with strategy)
  - [_hash_ovflblkno_to_bitno](_hash_ovflblkno_to_bitno.md) (block number to bit number conversion)
  - [_hash_pgaddmultitup](_hash_pgaddmultitup.md) (adding multiple tuples to a page)
  - [_hash_pageinit](_hash_pageinit.md) (page initialization)
  - CLRBIT/ISSET (bitmap manipulation)
  - XLog functions (WAL logging)
  - [_hash_relbuf](_hash_relbuf.md) (buffer release)
- Called from (representative examples):
  - [_hash_squeezebucket](_hash_squeezebucket.md) (during bucket squeeze operations)
  - HASHNProcs (hash index procedure definitions)

## Notes and Other Information
- The function releases the write lock on ovflbuf before exiting
- Uses lock chaining strategy to prevent deadlocks during concurrent operations
- Returns the block number of the page that followed the freed page in the bucket chain
- Includes comprehensive WAL logging with support for multiple buffer registrations
- The bstrategy parameter controls buffer access for bucket pages but is intentionally not used for metapage and bitmap access
- Performs validation to ensure the overflow bit number is valid before clearing it
- Updates the hashm_firstfree pointer in metadata when the freed page becomes the earliest free page
- Critical section ensures atomicity of all modifications across multiple pages

## Simplified Source

```c
// Remove an overflow page from its bucket chain and mark it as free.
// Transfer any remaining tuples to the designated write page.
BlockNumber _hash_freeovflpage(Relation rel, Buffer bucketbuf, Buffer ovflbuf,
                              Buffer wbuf, IndexTuple *itups, OffsetNumber *itup_offsets,
                              Size *tups_size, uint16 nitups,
                              BufferAccessStrategy bstrategy) {
    HashMetaPage metap;
    Buffer metabuf, mapbuf;
    BlockNumber ovflblkno, prevblkno, nextblkno, writeblkno;
    HashPageOpaque ovflopaque;
    Page ovflpage, mappage;
    uint32 *freep, ovflbitno;
    int32 bitmappage, bitmapbit;
    Buffer prevbuf = InvalidBuffer, nextbuf = InvalidBuffer;
    bool update_metap = false;

    // Get information from the overflow page being freed
    _hash_checkpage(rel, ovflbuf, LH_OVERFLOW_PAGE);
    ovflblkno = BufferGetBlockNumber(ovflbuf);
    ovflpage = BufferGetPage(ovflbuf);
    ovflopaque = HashPageGetOpaque(ovflpage);
    nextblkno = ovflopaque->hasho_nextblkno;
    prevblkno = ovflopaque->hasho_prevblkno;
    writeblkno = BufferGetBlockNumber(wbuf);

    // Fix up the bucket chain - get buffers for prev/next pages
    if (BlockNumberIsValid(prevblkno)) {
        if (prevblkno == writeblkno)
            prevbuf = wbuf;  // Previous page is same as write buffer
        else
            prevbuf = _hash_getbuf_with_strategy(rel, prevblkno, HASH_WRITE,
                                               LH_BUCKET_PAGE | LH_OVERFLOW_PAGE,
                                               bstrategy);
    }
    if (BlockNumberIsValid(nextblkno))
        nextbuf = _hash_getbuf_with_strategy(rel, nextblkno, HASH_WRITE,
                                           LH_OVERFLOW_PAGE, bstrategy);

    // Read metapage to identify which bitmap page to use
    metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_READ, LH_META_PAGE);
    metap = HashPageGetMeta(BufferGetPage(metabuf));

    // Calculate bitmap location for this overflow page
    ovflbitno = _hash_ovflblkno_to_bitno(metap, ovflblkno);
    bitmappage = ovflbitno >> BMPG_SHIFT(metap);
    bitmapbit = ovflbitno & BMPG_MASK(metap);

    if (bitmappage >= metap->hashm_nmaps)
        elog(ERROR, "invalid overflow bit number %u", ovflbitno);

    BlockNumber blkno = metap->hashm_mapp[bitmappage];
    LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);

    // Get bitmap page and prepare for multiple WAL records
    mapbuf = _hash_getbuf(rel, blkno, HASH_WRITE, LH_BITMAP_PAGE);
    mappage = BufferGetPage(mapbuf);
    freep = HashPageGetBitmap(mappage);
    Assert(ISSET(freep, bitmapbit));

    // Get write-lock on metapage to update firstfree
    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

    if (RelationNeedsWAL(rel))
        XLogEnsureRecordSpace(HASH_XLOG_FREE_OVFL_BUFS, 4 + nitups);

    START_CRIT_SECTION();

    // Add tuples to write page (preserving hashkey ordering)
    if (nitups > 0) {
        _hash_pgaddmultitup(rel, wbuf, itups, itup_offsets, nitups);
        MarkBufferDirty(wbuf);
    }

    // Reinitialize the freed overflow page
    _hash_pageinit(ovflpage, BufferGetPageSize(ovflbuf));
    ovflopaque = HashPageGetOpaque(ovflpage);
    ovflopaque->hasho_prevblkno = InvalidBlockNumber;
    ovflopaque->hasho_nextblkno = InvalidBlockNumber;
    ovflopaque->hasho_bucket = InvalidBucket;
    ovflopaque->hasho_flag = LH_UNUSED_PAGE;
    ovflopaque->hasho_page_id = HASHO_PAGE_ID;
    MarkBufferDirty(ovflbuf);

    // Update previous page to point to next page
    if (BufferIsValid(prevbuf)) {
        Page prevpage = BufferGetPage(prevbuf);
        HashPageOpaque prevopaque = HashPageGetOpaque(prevpage);
        prevopaque->hasho_nextblkno = nextblkno;
        MarkBufferDirty(prevbuf);
    }

    // Update next page to point to previous page
    if (BufferIsValid(nextbuf)) {
        Page nextpage = BufferGetPage(nextbuf);
        HashPageOpaque nextopaque = HashPageGetOpaque(nextpage);
        nextopaque->hasho_prevblkno = prevblkno;
        MarkBufferDirty(nextbuf);
    }

    // Clear the bitmap bit to mark page as free
    CLRBIT(freep, bitmapbit);
    MarkBufferDirty(mapbuf);

    // Update firstfree pointer if this becomes the earliest free page
    if (ovflbitno < metap->hashm_firstfree) {
        metap->hashm_firstfree = ovflbitno;
        update_metap = true;
        MarkBufferDirty(metabuf);
    }

    // WAL logging for crash recovery and replication
    if (RelationNeedsWAL(rel)) {
        xl_hash_squeeze_page xlrec;
        xlrec.prevblkno = prevblkno;
        xlrec.nextblkno = nextblkno;
        xlrec.ntups = nitups;
        xlrec.is_prim_bucket_same_wrt = (wbuf == bucketbuf);
        xlrec.is_prev_bucket_same_wrt = (wbuf == prevbuf);

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfHashSqueezePage);

        // Register buffers based on what changed
        if (!xlrec.is_prim_bucket_same_wrt) {
            uint8 flags = REGBUF_STANDARD | REGBUF_NO_IMAGE | REGBUF_NO_CHANGE;
            XLogRegisterBuffer(0, bucketbuf, flags);
        }

        if (xlrec.ntups > 0) {
            XLogRegisterBuffer(1, wbuf, REGBUF_STANDARD);
            XLogRegisterBufData(1, (char *) itup_offsets,
                              nitups * sizeof(OffsetNumber));
            for (int i = 0; i < nitups; i++)
                XLogRegisterBufData(1, (char *) itups[i], tups_size[i]);
        } else if (xlrec.is_prim_bucket_same_wrt || xlrec.is_prev_bucket_same_wrt) {
            uint8 wbuf_flags = REGBUF_STANDARD;
            if (!xlrec.is_prev_bucket_same_wrt)
                wbuf_flags |= REGBUF_NO_CHANGE;
            XLogRegisterBuffer(1, wbuf, wbuf_flags);
        }

        XLogRegisterBuffer(2, ovflbuf, REGBUF_STANDARD);

        if (BufferIsValid(prevbuf) && !xlrec.is_prev_bucket_same_wrt)
            XLogRegisterBuffer(3, prevbuf, REGBUF_STANDARD);
        if (BufferIsValid(nextbuf))
            XLogRegisterBuffer(4, nextbuf, REGBUF_STANDARD);

        XLogRegisterBuffer(5, mapbuf, REGBUF_STANDARD);
        XLogRegisterBufData(5, (char *) &bitmapbit, sizeof(uint32));

        if (update_metap) {
            XLogRegisterBuffer(6, metabuf, REGBUF_STANDARD);
            XLogRegisterBufData(6, (char *) &metap->hashm_firstfree, sizeof(uint32));
        }

        XLogRecPtr recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_SQUEEZE_PAGE);

        // Set LSN on modified pages
        if (xlrec.ntups > 0 || xlrec.is_prev_bucket_same_wrt)
            PageSetLSN(BufferGetPage(wbuf), recptr);
        PageSetLSN(BufferGetPage(ovflbuf), recptr);
        if (BufferIsValid(prevbuf) && !xlrec.is_prev_bucket_same_wrt)
            PageSetLSN(BufferGetPage(prevbuf), recptr);
        if (BufferIsValid(nextbuf))
            PageSetLSN(BufferGetPage(nextbuf), recptr);
        PageSetLSN(BufferGetPage(mapbuf), recptr);
        if (update_metap)
            PageSetLSN(BufferGetPage(metabuf), recptr);
    }

    END_CRIT_SECTION();

    // Release all buffers
    if (BufferIsValid(prevbuf) && prevblkno != writeblkno)
        _hash_relbuf(rel, prevbuf);
    if (BufferIsValid(ovflbuf))
        _hash_relbuf(rel, ovflbuf);
    if (BufferIsValid(nextbuf))
        _hash_relbuf(rel, nextbuf);
    _hash_relbuf(rel, mapbuf);
    _hash_relbuf(rel, metabuf);

    return nextblkno;
}
```