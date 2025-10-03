# _hash_init

## Location
[src/backend/access/hash/hashpage.c:327-497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L327-L497)

## Overview
This function initializes a new hash index by creating and setting up the metadata page, initial bucket pages, and the first bitmap page, establishing the foundational structure for hash index operations.

## Definition

```c
uint32
_hash_init(Relation rel, double num_tuples, ForkNumber forkNum)
```
## Detailed Description
 is the primary initialization function for hash indexes that performs comprehensive setup of the index structure. It calculates an appropriate number of initial buckets based on the estimated tuple count and target fill factor, then creates and initializes the metadata page, all initial bucket pages, and the first bitmap page. The function uses WAL logging when appropriate to ensure crash safety. The initialization process involves careful buffer management and follows a specific sequence to ensure the storage manager has the correct understanding of the physical index length.

## Parameters / Member Variables
- `rel`: The relation (hash index) being initialized
- `num_tuples`: Estimated number of tuples to be loaded into the index initially
- `forkNum`: The fork number specifying which fork of the relation to initialize
## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetNumberOfBlocksInFork](../R/RelationGetNumberOfBlocksInFork.md) (safety check for empty index)
  - RelationNeedsWAL (WAL logging determination)
  - HashGetTargetPageUsage (fill factor calculation)
  - [index_getprocid](../i/index_getprocid.md) (hash function procedure lookup)
  - [_hash_getnewbuf](_hash_getnewbuf.md) (buffer allocation)
  - [_hash_init_metabuffer](_hash_init_metabuffer.md) (metadata page initialization)
  - [_hash_initbuf](_hash_initbuf.md) (bucket page initialization)
  - [_hash_initbitmapbuffer](_hash_initbitmapbuffer.md) (bitmap page initialization)
  - [_hash_relbuf](_hash_relbuf.md) (buffer release)
  - [XLogInsert](../X/XLogInsert.md)/XLogBeginInsert (WAL logging)
  - HashPageGetMeta (metadata page access)
  - BUCKET_TO_BLKNO (block number calculation)
  - [LockBuffer](../L/LockBuffer.md)/MarkBufferDirty (buffer management)

- Called from (representative examples):
  - [hashbuild](hashbuild.md) (index creation during BUILD)
  - [hashbuildempty](hashbuildempty.md) (empty index creation)

## Notes and Other Information
- The function performs a safety check to ensure the index is completely empty before initialization
- Calculates optimal initial bucket count based on estimated tuple count and target fill factor (minimum 10 tuples per bucket)
- WAL logs all operations when the relation is persistent or when initializing the init fork
- Uses relaxed locking rules during initialization since no concurrent access is possible
- Temporarily releases the metadata buffer lock during bucket initialization to allow interrupts and prevent blocking the background writer
- Creates the first bitmap page immediately after bucket creation and registers it in the metadata
- Returns the number of buckets created, which can be used by calling functions
- The initialization sequence (metadata → buckets → bitmap) is important for storage manager consistency
- Includes comprehensive error handling for resource limits (e.g., maximum bitmap pages)
- All buffer operations are properly WAL-logged for crash recovery when needed

## Simplified Source

```c
uint32 _hash_init(Relation rel, double num_tuples, ForkNumber forkNum)
{
    Buffer metabuf, buf, bitmapbuf;
    Page pg;
    HashMetaPage metap;
    uint32 num_buckets, i;
    bool use_wal;

    // Safety check - index must be empty
    if (RelationGetNumberOfBlocksInFork(rel, forkNum) != 0)
        elog(ERROR, "cannot initialize non-empty hash index");

    // Determine if WAL logging is needed
    use_wal = RelationNeedsWAL(rel) || forkNum == INIT_FORKNUM;

    // Calculate optimal fill factor based on tuple size
    int32 data_width = sizeof(uint32);
    int32 item_width = MAXALIGN(sizeof(IndexTupleData)) + MAXALIGN(data_width) + sizeof(ItemIdData);
    int32 ffactor = HashGetTargetPageUsage(rel) / item_width;
    if (ffactor < 10) ffactor = 10;

    // Get hash function procedure
    RegProcedure procid = index_getprocid(rel, 1, HASHSTANDARD_PROC);

    // Initialize metadata page
    metabuf = _hash_getnewbuf(rel, HASH_METAPAGE, forkNum);
    _hash_init_metabuffer(metabuf, num_tuples, procid, ffactor, false);
    MarkBufferDirty(metabuf);

    // WAL log metadata initialization
    if (use_wal) {
        xl_hash_init_meta_page xlrec;
        pg = BufferGetPage(metabuf);
        metap = HashPageGetMeta(pg);
        xlrec.num_tuples = num_tuples;
        xlrec.procid = metap->hashm_procid;
        xlrec.ffactor = metap->hashm_ffactor;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfHashInitMetaPage);
        XLogRegisterBuffer(0, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);
        XLogRecPtr recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_INIT_META_PAGE);
        PageSetLSN(BufferGetPage(metabuf), recptr);
    }

    pg = BufferGetPage(metabuf);
    metap = HashPageGetMeta(pg);
    num_buckets = metap->hashm_maxbucket + 1;

    // Release metapage lock during bucket creation to allow interrupts
    LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);

    // Initialize all initial bucket pages
    for (i = 0; i < num_buckets; i++) {
        CHECK_FOR_INTERRUPTS();

        BlockNumber blkno = BUCKET_TO_BLKNO(metap, i);
        buf = _hash_getnewbuf(rel, blkno, forkNum);
        _hash_initbuf(buf, metap->hashm_maxbucket, i, LH_BUCKET_PAGE, false);
        MarkBufferDirty(buf);

        if (use_wal)
            log_newpage(&rel->rd_locator, forkNum, blkno, BufferGetPage(buf), true);
        _hash_relbuf(rel, buf);
    }

    // Reacquire metapage lock
    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

    // Initialize bitmap page
    bitmapbuf = _hash_getnewbuf(rel, num_buckets + 1, forkNum);
    _hash_initbitmapbuffer(bitmapbuf, metap->hashm_bmsize, false);
    MarkBufferDirty(bitmapbuf);

    // Add bitmap page to metadata
    if (metap->hashm_nmaps >= HASH_MAX_BITMAPS)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       errmsg("out of overflow pages in hash index")));

    metap->hashm_mapp[metap->hashm_nmaps] = num_buckets + 1;
    metap->hashm_nmaps++;
    MarkBufferDirty(metabuf);

    // WAL log bitmap initialization
    if (use_wal) {
        xl_hash_init_bitmap_page xlrec;
        xlrec.bmsize = metap->hashm_bmsize;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfHashInitBitmapPage);
        XLogRegisterBuffer(0, bitmapbuf, REGBUF_WILL_INIT);
        XLogRegisterBuffer(1, metabuf, REGBUF_STANDARD);
        XLogRecPtr recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_INIT_BITMAP_PAGE);
        PageSetLSN(BufferGetPage(bitmapbuf), recptr);
        PageSetLSN(BufferGetPage(metabuf), recptr);
    }

    // Clean up and return
    _hash_relbuf(rel, bitmapbuf);
    _hash_relbuf(rel, metabuf);

    return num_buckets;
}
```