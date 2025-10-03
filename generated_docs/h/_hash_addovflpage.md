# _hash_addovflpage

## Location
[src/backend/access/hash/hashovfl.c:112-447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashovfl.c#L112-L447)

## Overview
Adds a new overflow page to a hash bucket chain, handling both recycling of previously freed pages and allocation of new pages when needed.

## Definition
```c
Buffer _hash_addovflpage(Relation rel, Buffer metabuf, Buffer buf, bool retain_pin)
```

## Detailed Description
This function is responsible for extending a hash bucket chain by adding an overflow page. It performs a comprehensive operation that includes:

1. **Finding the tail page**: Traverses the bucket chain to locate the current last page
2. **Searching for free pages**: Scans bitmap pages to find recyclable overflow pages, starting from the hashm_firstfree position
3. **Allocating new pages**: When no free pages are available, extends the relation by allocating new overflow pages and potentially new bitmap pages
4. **Proper locking**: Maintains a strict locking order (tail page → meta page → bitmap page → overflow page) to prevent deadlocks
5. **WAL logging**: Creates a single WAL record covering all changes to ensure atomicity

The function is designed to handle concurrent access safely and includes comprehensive error handling and validation.

## Parameters / Member Variables
- `rel`: The hash index relation being modified
- `metabuf`: Buffer containing the metadata page (caller must hold pin, no lock required)
- `buf`: Buffer pointing to the current last page of the bucket chain (caller must hold pin, no lock required)
- `retain_pin`: Whether to retain the pin on the primary bucket page after completion

## Dependencies
- Functions called/Symbols referenced:
  - [LockBuffer](../L/LockBuffer.md)/BUFFER_LOCK_EXCLUSIVE/BUFFER_LOCK_UNLOCK (buffer locking)
  - [_hash_checkpage](_hash_checkpage.md) (page validation)
  - HashPageGetOpaque/HashPageGetMeta/HashPageGetBitmap (page access)
  - [_hash_getbuf](_hash_getbuf.md)/_hash_getinitbuf/_hash_getnewbuf (buffer management)
  - [_hash_relbuf](_hash_relbuf.md) (buffer release)
  - [bitno_to_blkno](../b/bitno_to_blkno.md) (bit number to block number conversion)
  - [_hash_firstfreebit](_hash_firstfreebit.md) (finding first free bit in bitmap)
  - [_hash_initbitmapbuffer](_hash_initbitmapbuffer.md) (initializing new bitmap pages)
  - SETBIT (setting bits in bitmap)
  - XLog functions (WAL logging)
- Called from (representative examples):
  - [_hash_doinsert](_hash_doinsert.md) (during tuple insertion when bucket is full)
  - [_hash_splitbucket](_hash_splitbucket.md) (during bucket splitting operations)
  - HASHNProcs (hash index procedure definitions)

## Notes and Other Information
- The function maintains strict locking order to prevent deadlocks with concurrent operations
- Returns a pinned and write-locked overflow page that is guaranteed to be empty
- Handles bitmap page allocation when the current bitmap pages are exhausted
- Uses a single WAL record for all changes to prevent partial updates in case of crashes
- The retain_pin parameter is typically true only for primary bucket pages
- Includes comprehensive validation and error reporting for bitmap limits
- The function may traverse multiple overflow pages if other processes added pages concurrently

## Simplified Source

```c
// Add an overflow page to the bucket whose last page is pointed to by 'buf'.
// Returns a pinned and write-locked overflow page that is guaranteed to be empty.
Buffer _hash_addovflpage(Relation rel, Buffer metabuf, Buffer buf, bool retain_pin) {
    Buffer ovflbuf, mapbuf = InvalidBuffer, newmapbuf = InvalidBuffer;
    Page page, ovflpage;
    HashPageOpaque pageopaque, ovflopaque;
    HashMetaPage metap;
    BlockNumber blkno;
    uint32 orig_firstfree, splitnum, *freep = NULL;
    uint32 max_ovflpg, bit, bitmap_page_bit;
    uint32 first_page, last_bit, last_page;
    uint32 i, j;
    bool page_found = false;

    // Write-lock the tail page and find the actual end of bucket chain
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    _hash_checkpage(rel, buf, LH_BUCKET_PAGE | LH_OVERFLOW_PAGE);

    // Traverse to find current tail page (in case someone else added pages)
    for (;;) {
        page = BufferGetPage(buf);
        pageopaque = HashPageGetOpaque(page);
        BlockNumber nextblkno = pageopaque->hasho_nextblkno;

        if (!BlockNumberIsValid(nextblkno))
            break;

        // Move to next page in chain
        if (retain_pin) {
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            retain_pin = false;
        } else {
            _hash_relbuf(rel, buf);
        }
        buf = _hash_getbuf(rel, nextblkno, HASH_WRITE, LH_OVERFLOW_PAGE);
    }

    // Get exclusive lock on metapage and search for free pages
    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
    _hash_checkpage(rel, metabuf, LH_META_PAGE);
    metap = HashPageGetMeta(BufferGetPage(metabuf));

    // Search bitmap pages for free overflow pages
    orig_firstfree = metap->hashm_firstfree;
    first_page = orig_firstfree >> BMPG_SHIFT(metap);
    bit = orig_firstfree & BMPG_MASK(metap);
    i = first_page;
    j = bit / BITS_PER_MAP;
    bit &= ~(BITS_PER_MAP - 1);

    // Search existing bitmap pages for free space
    for (;;) {
        splitnum = metap->hashm_ovflpoint;
        max_ovflpg = metap->hashm_spares[splitnum] - 1;
        last_page = max_ovflpg >> BMPG_SHIFT(metap);
        last_bit = max_ovflpg & BMPG_MASK(metap);

        if (i > last_page)
            break;

        BlockNumber mapblkno = metap->hashm_mapp[i];
        uint32 last_inpage = (i == last_page) ? last_bit : BMPGSZ_BIT(metap) - 1;

        LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
        mapbuf = _hash_getbuf(rel, mapblkno, HASH_WRITE, LH_BITMAP_PAGE);
        Page mappage = BufferGetPage(mapbuf);
        freep = HashPageGetBitmap(mappage);

        // Search for free bit in this bitmap page
        for (; bit <= last_inpage; j++, bit += BITS_PER_MAP) {
            if (freep[j] != ALL_SET) {
                page_found = true;
                LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

                bit += _hash_firstfreebit(freep[j]);
                bitmap_page_bit = bit;
                bit += (i << BMPG_SHIFT(metap));
                blkno = bitno_to_blkno(metap, bit);

                ovflbuf = _hash_getinitbuf(rel, blkno);
                goto found;
            }
        }

        // No free space, try next bitmap page
        _hash_relbuf(rel, mapbuf);
        mapbuf = InvalidBuffer;
        i++; j = 0; bit = 0;
        LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
    }

    // No free pages - extend relation with new overflow page
    if (last_bit == (uint32) (BMPGSZ_BIT(metap) - 1)) {
        // Need new bitmap page too
        bit = metap->hashm_spares[splitnum];
        if (metap->hashm_nmaps >= HASH_MAX_BITMAPS)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                           errmsg("out of overflow pages in hash index \"%s\"",
                                 RelationGetRelationName(rel))));
        newmapbuf = _hash_getnewbuf(rel, bitno_to_blkno(metap, bit), MAIN_FORKNUM);
    }

    // Calculate address of new overflow page
    bit = BufferIsValid(newmapbuf) ?
        metap->hashm_spares[splitnum] + 1 : metap->hashm_spares[splitnum];
    blkno = bitno_to_blkno(metap, bit);
    ovflbuf = _hash_getnewbuf(rel, blkno, MAIN_FORKNUM);

found:
    // Update structures atomically
    START_CRIT_SECTION();

    if (page_found) {
        // Mark recycled page as "in use"
        SETBIT(freep, bitmap_page_bit);
        MarkBufferDirty(mapbuf);
    } else {
        // Update count for new overflow page
        metap->hashm_spares[splitnum]++;

        if (BufferIsValid(newmapbuf)) {
            _hash_initbitmapbuffer(newmapbuf, metap->hashm_bmsize, false);
            MarkBufferDirty(newmapbuf);
            metap->hashm_mapp[metap->hashm_nmaps] = BufferGetBlockNumber(newmapbuf);
            metap->hashm_nmaps++;
            metap->hashm_spares[splitnum]++;
        }
        MarkBufferDirty(metabuf);
    }

    // Update firstfree pointer if we used the first free page
    if (metap->hashm_firstfree == orig_firstfree) {
        metap->hashm_firstfree = bit + 1;
        MarkBufferDirty(metabuf);
    }

    // Initialize new overflow page and link it to chain
    ovflpage = BufferGetPage(ovflbuf);
    ovflopaque = HashPageGetOpaque(ovflpage);
    ovflopaque->hasho_prevblkno = BufferGetBlockNumber(buf);
    ovflopaque->hasho_nextblkno = InvalidBlockNumber;
    ovflopaque->hasho_bucket = pageopaque->hasho_bucket;
    ovflopaque->hasho_flag = LH_OVERFLOW_PAGE;
    ovflopaque->hasho_page_id = HASHO_PAGE_ID;
    MarkBufferDirty(ovflbuf);

    // Link tail page to new overflow page
    pageopaque->hasho_nextblkno = BufferGetBlockNumber(ovflbuf);
    MarkBufferDirty(buf);

    // WAL logging for atomicity
    if (RelationNeedsWAL(rel)) {
        xl_hash_add_ovfl_page xlrec;
        xlrec.bmpage_found = page_found;
        xlrec.bmsize = metap->hashm_bmsize;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfHashAddOvflPage);
        XLogRegisterBuffer(0, ovflbuf, REGBUF_WILL_INIT);
        XLogRegisterBufData(0, (char *) &pageopaque->hasho_bucket, sizeof(Bucket));
        XLogRegisterBuffer(1, buf, REGBUF_STANDARD);

        if (BufferIsValid(mapbuf)) {
            XLogRegisterBuffer(2, mapbuf, REGBUF_STANDARD);
            XLogRegisterBufData(2, (char *) &bitmap_page_bit, sizeof(uint32));
        }
        if (BufferIsValid(newmapbuf))
            XLogRegisterBuffer(3, newmapbuf, REGBUF_WILL_INIT);

        XLogRegisterBuffer(4, metabuf, REGBUF_STANDARD);
        XLogRegisterBufData(4, (char *) &metap->hashm_firstfree, sizeof(uint32));

        XLogRecPtr recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_ADD_OVFL_PAGE);
        PageSetLSN(BufferGetPage(ovflbuf), recptr);
        PageSetLSN(BufferGetPage(buf), recptr);
        if (BufferIsValid(mapbuf))
            PageSetLSN(BufferGetPage(mapbuf), recptr);
        if (BufferIsValid(newmapbuf))
            PageSetLSN(BufferGetPage(newmapbuf), recptr);
        PageSetLSN(BufferGetPage(metabuf), recptr);
    }

    END_CRIT_SECTION();

    // Release buffers
    if (retain_pin)
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    else
        _hash_relbuf(rel, buf);

    if (BufferIsValid(mapbuf))
        _hash_relbuf(rel, mapbuf);
    LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
    if (BufferIsValid(newmapbuf))
        _hash_relbuf(rel, newmapbuf);

    return ovflbuf;
}
```