# _hash_expandtable

## Location
[src/backend/access/hash/hashpage.c:614-991](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L614-L991)

## Overview
Attempts to expand the hash table by creating one new bucket, handling the complex process of bucket splitting with proper locking and crash recovery support.

## Definition
```c
void _hash_expandtable(Relation rel, Buffer metabuf)
```

## Detailed Description
This function implements the core hash table expansion algorithm that creates a new bucket and redistributes tuples from an existing bucket. It performs comprehensive checks before proceeding with the split, including verifying that a split is still needed, handling any pending splits or cleanup operations, and ensuring proper locking. The function maintains crash safety through WAL logging and uses a restart mechanism to handle concurrent operations. If the split point increases, it allocates new bucket pages in batches. The actual tuple redistribution is delegated to _hash_splitbucket after updating metadata and marking buckets appropriately.

## Parameters / Member Variables
- `rel`: The hash index relation being expanded
- `metabuf`: Buffer containing the metadata page (caller must hold pin but no lock)

## Dependencies
- Functions called/Symbols referenced:
  - [_hash_checkpage](_hash_checkpage.md)
  - HashPageGetMeta
  - BUCKET_TO_BLKNO
  - [_hash_getbuf_with_condlock_cleanup](_hash_getbuf_with_condlock_cleanup.md)
  - [_hash_finish_split](_hash_finish_split.md)
  - [_hash_dropbuf](_hash_dropbuf.md)
  - [hashbucketcleanup](hashbucketcleanup.md)
  - [_hash_spareindex](_hash_spareindex.md)
  - [_hash_get_totalbuckets](_hash_get_totalbuckets.md)
  - [_hash_alloc_buckets](_hash_alloc_buckets.md)
  - [_hash_getnewbuf](_hash_getnewbuf.md)
  - [_hash_splitbucket](_hash_splitbucket.md)
  - Various WAL logging functions (XLogInsert, XLogRegisterBuffer, etc.)
- Called from (representative examples):
  - [_hash_doinsert](_hash_doinsert.md)

## Notes and Other Information
- Silently does nothing if cleanup locks cannot be acquired on old or new buckets
- Uses restart mechanism to handle concurrent splits and cleanup operations
- Maintains strict upper limit of 0x7FFFFFFE buckets to prevent overflow
- Implements comprehensive WAL logging for crash recovery
- Updates metadata including bucket masks and overflow point when creating new splitpoints
- Uses critical sections to ensure atomic updates of shared buffer pages
- Handles both simple bucket splits and complex splitpoint increases requiring batch allocation

## Simplified Source

```c
void _hash_expandtable(Relation rel, Buffer metabuf)
{
    HashMetaPage metap;
    Bucket old_bucket, new_bucket;
    Buffer buf_oblkno, buf_nblkno;
    bool metap_update_masks = false;
    bool metap_update_splitpoint = false;

restart_expand:
    // Lock metadata page and check if split is still needed
    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
    metap = HashPageGetMeta(BufferGetPage(metabuf));

    // Check if someone else already did the split
    if (metap->hashm_ntuples <= (double) metap->hashm_ffactor * (metap->hashm_maxbucket + 1))
        goto fail;

    // Check bucket limit to prevent overflow
    if (metap->hashm_maxbucket >= (uint32) 0x7FFFFFFE)
        goto fail;

    // Determine buckets and try to get cleanup lock on old bucket
    new_bucket = metap->hashm_maxbucket + 1;
    old_bucket = (new_bucket & metap->hashm_lowmask);
    BlockNumber start_oblkno = BUCKET_TO_BLKNO(metap, old_bucket);

    buf_oblkno = _hash_getbuf_with_condlock_cleanup(rel, start_oblkno, LH_BUCKET_PAGE);
    if (!buf_oblkno)
        goto fail;

    // Handle pending splits or cleanup
    HashPageOpaque oopaque = HashPageGetOpaque(BufferGetPage(buf_oblkno));
    if (H_BUCKET_BEING_SPLIT(oopaque)) {
        // Complete the pending split first
        uint32 maxbucket = metap->hashm_maxbucket;
        uint32 highmask = metap->hashm_highmask;
        uint32 lowmask = metap->hashm_lowmask;

        LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
        LockBuffer(buf_oblkno, BUFFER_LOCK_UNLOCK);

        _hash_finish_split(rel, metabuf, buf_oblkno, old_bucket, maxbucket, highmask, lowmask);
        _hash_dropbuf(rel, buf_oblkno);
        goto restart_expand;
    }

    if (H_NEEDS_SPLIT_CLEANUP(oopaque)) {
        // Clean up from previous split
        uint32 maxbucket = metap->hashm_maxbucket;
        uint32 highmask = metap->hashm_highmask;
        uint32 lowmask = metap->hashm_lowmask;

        LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
        hashbucketcleanup(rel, old_bucket, buf_oblkno, start_oblkno, NULL,
                         maxbucket, highmask, lowmask, NULL, NULL, true, NULL, NULL);
        _hash_dropbuf(rel, buf_oblkno);
        goto restart_expand;
    }

    // Allocate new bucket pages if needed for splitpoint increase
    BlockNumber start_nblkno = BUCKET_TO_BLKNO(metap, new_bucket);
    uint32 spare_ndx = _hash_spareindex(new_bucket + 1);
    if (spare_ndx > metap->hashm_ovflpoint) {
        uint32 buckets_to_add = _hash_get_totalbuckets(spare_ndx) - new_bucket;
        if (!_hash_alloc_buckets(rel, start_nblkno, buckets_to_add)) {
            _hash_relbuf(rel, buf_oblkno);
            goto fail;
        }
    }

    // Allocate new bucket's primary page
    buf_nblkno = _hash_getnewbuf(rel, start_nblkno, MAIN_FORKNUM);
    if (!IsBufferCleanupOK(buf_nblkno)) {
        _hash_relbuf(rel, buf_oblkno);
        _hash_relbuf(rel, buf_nblkno);
        goto fail;
    }

    START_CRIT_SECTION();

    // Update metadata
    metap->hashm_maxbucket = new_bucket;
    if (new_bucket > metap->hashm_highmask) {
        metap->hashm_lowmask = metap->hashm_highmask;
        metap->hashm_highmask = new_bucket | metap->hashm_lowmask;
        metap_update_masks = true;
    }

    if (spare_ndx > metap->hashm_ovflpoint) {
        metap->hashm_spares[spare_ndx] = metap->hashm_spares[metap->hashm_ovflpoint];
        metap->hashm_ovflpoint = spare_ndx;
        metap_update_splitpoint = true;
    }

    MarkBufferDirty(metabuf);

    // Mark buckets as being split/populated
    oopaque->hasho_flag |= LH_BUCKET_BEING_SPLIT;
    oopaque->hasho_prevblkno = metap->hashm_maxbucket;
    MarkBufferDirty(buf_oblkno);

    // Initialize new bucket page
    Page npage = BufferGetPage(buf_nblkno);
    HashPageOpaque nopaque = HashPageGetOpaque(npage);
    nopaque->hasho_prevblkno = metap->hashm_maxbucket;
    nopaque->hasho_nextblkno = InvalidBlockNumber;
    nopaque->hasho_bucket = new_bucket;
    nopaque->hasho_flag = LH_BUCKET_PAGE | LH_BUCKET_BEING_POPULATED;
    nopaque->hasho_page_id = HASHO_PAGE_ID;
    MarkBufferDirty(buf_nblkno);

    // WAL logging if needed
    if (RelationNeedsWAL(rel)) {
        xl_hash_split_allocate_page xlrec;
        xlrec.new_bucket = metap->hashm_maxbucket;
        xlrec.old_bucket_flag = oopaque->hasho_flag;
        xlrec.new_bucket_flag = nopaque->hasho_flag;
        xlrec.flags = 0;

        XLogBeginInsert();
        XLogRegisterBuffer(0, buf_oblkno, REGBUF_STANDARD);
        XLogRegisterBuffer(1, buf_nblkno, REGBUF_WILL_INIT);
        XLogRegisterBuffer(2, metabuf, REGBUF_STANDARD);

        if (metap_update_masks) {
            xlrec.flags |= XLH_SPLIT_META_UPDATE_MASKS;
            XLogRegisterBufData(2, (char *) &metap->hashm_lowmask, sizeof(uint32));
            XLogRegisterBufData(2, (char *) &metap->hashm_highmask, sizeof(uint32));
        }

        if (metap_update_splitpoint) {
            xlrec.flags |= XLH_SPLIT_META_UPDATE_SPLITPOINT;
            XLogRegisterBufData(2, (char *) &metap->hashm_ovflpoint, sizeof(uint32));
            XLogRegisterBufData(2, (char *) &metap->hashm_spares[metap->hashm_ovflpoint], sizeof(uint32));
        }

        XLogRegisterData((char *) &xlrec, SizeOfHashSplitAllocPage);
        XLogRecPtr recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_SPLIT_ALLOCATE_PAGE);

        PageSetLSN(BufferGetPage(buf_oblkno), recptr);
        PageSetLSN(BufferGetPage(buf_nblkno), recptr);
        PageSetLSN(BufferGetPage(metabuf), recptr);
    }

    END_CRIT_SECTION();

    // Release metapage lock and perform the split
    LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);

    uint32 maxbucket = metap->hashm_maxbucket;
    uint32 highmask = metap->hashm_highmask;
    uint32 lowmask = metap->hashm_lowmask;

    _hash_splitbucket(rel, metabuf, old_bucket, new_bucket,
                     buf_oblkno, buf_nblkno, NULL,
                     maxbucket, highmask, lowmask);

    // Clean up
    _hash_dropbuf(rel, buf_oblkno);
    _hash_dropbuf(rel, buf_nblkno);
    return;

fail:
    LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
}
```