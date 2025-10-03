# hashbucketcleanup

## Location
[src/backend/access/hash/hash.c:687-927](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash.c#L687-L927)

## Overview
Performs deletion of index entries from a specific hash bucket, handling both regular tuple deletion via callback and cleanup of tuples moved by bucket split operations.

## Definition

```c
void
hashbucketcleanup(Relation rel, Bucket cur_bucket, Buffer bucket_buf,
				  BlockNumber bucket_blkno, BufferAccessStrategy bstrategy,
				  uint32 maxbucket, uint32 highmask, uint32 lowmask,
				  double *tuples_removed, double *num_index_tuples,
				  bool split_cleanup,
				  IndexBulkDeleteCallback callback, void *callback_state)
```
## Detailed Description
The hashbucketcleanup function is a comprehensive helper function that performs the actual tuple deletion work for a single hash bucket. It operates on the entire bucket chain, including all overflow pages, systematically scanning each tuple and determining whether it should be deleted based on either callback criteria (for regular VACUUM operations) or split cleanup requirements.

The function implements a sophisticated locking protocol to prevent concurrent scans from interfering with the cleanup process. It uses lock chaining, where it locks the next page in the bucket chain before releasing the lock on the previous page. This ensures that no concurrent scan can pass the cleanup scan and potentially see a tuple that is about to be deleted.

For split cleanup operations, the function identifies tuples that were moved to other buckets during split operations but remain in the original bucket. These tuples are marked for deletion to maintain hash index consistency. The function also handles clearing the garbage flag from buckets after split cleanup is complete.

The function implements WAL logging for all modifications, ensuring crash recovery consistency. After deletion operations are complete, it attempts to squeeze the bucket to compact free space, but only when a cleanup lock can be obtained without blocking.

## Parameters / Member Variables
- `rel`: The hash index relation being cleaned up
- `cur_bucket`: The bucket number being processed
- `bucket_buf`: Buffer containing the primary bucket page
- `bucket_blkno`: Block number of the primary bucket page
- `bstrategy`: Buffer access strategy for the operation
- `maxbucket`: Maximum bucket number in the hash index
- `highmask`: High-order bits mask for hash bucket calculation
- `lowmask`: Low-order bits mask for hash bucket calculation
- `*tuples_removed`: Pointer to counter for tracking number of tuples removed
- `*num_index_tuples`: Pointer to counter for tracking total number of tuples
- `split_cleanup`: Boolean flag indicating whether to perform split cleanup
- `callback`: Function pointer for determining which tuples to delete
- `*callback_state`: Opaque state data passed to the callback function
## Dependencies
- Functions called/Symbols referenced:
  - [_hash_get_newbucket_from_oldbucket](_hash_get_newbucket_from_oldbucket.md)
  - [vacuum_delay_point](../v/vacuum_delay_point.md)
  - HashPageGetOpaque
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [_hash_hashkey2bucket](_hash_hashkey2bucket.md)
  - [_hash_get_indextuple_hashkey](_hash_get_indextuple_hashkey.md)
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md)
  - H_HAS_DEAD_TUPLES
  - RelationNeedsWAL
  - [XLogInsert](../X/XLogInsert.md)
  - [_hash_getbuf_with_strategy](_hash_getbuf_with_strategy.md)
  - [_hash_relbuf](_hash_relbuf.md)
  - [IsBufferCleanupOK](../I/IsBufferCleanupOK.md)
  - [_hash_squeezebucket](_hash_squeezebucket.md)
- Called from (representative examples):
  - [hashbulkdelete](hashbulkdelete.md)
  - [_hash_expandtable](_hash_expandtable.md)
  - [_hash_splitbucket](_hash_splitbucket.md)

## Notes and Other Information
- Expects caller to hold cleanup lock on primary bucket page and returns with write lock held
- Uses lock chaining to prevent concurrent scans from interfering with cleanup process
- Retains pin on primary bucket page throughout the operation to prevent concurrent splits
- Handles both regular tuple deletion (via callback) and split cleanup operations
- Clears LH_PAGE_HAS_DEAD_TUPLES flag when removing dead tuples from pages
- Clears LH_BUCKET_NEEDS_SPLIT_CLEANUP flag after completing split cleanup
- Implements comprehensive WAL logging for crash recovery consistency
- Attempts bucket squeezing at the end if cleanup lock is available and deletions occurred
- Uses vacuum_delay_point() to allow vacuum throttling during long operations

## Simplified Source

```c
void hashbucketcleanup(Relation rel, Bucket cur_bucket, Buffer bucket_buf,
                      BlockNumber bucket_blkno, BufferAccessStrategy bstrategy,
                      uint32 maxbucket, uint32 highmask, uint32 lowmask,
                      double *tuples_removed, double *num_index_tuples,
                      bool split_cleanup,
                      IndexBulkDeleteCallback callback, void *callback_state) {
    BlockNumber blkno = bucket_blkno;
    Buffer buf = bucket_buf;
    Bucket new_bucket = InvalidBucket;
    bool bucket_dirty = false;

    // Determine new bucket for split cleanup
    if (split_cleanup)
        new_bucket = _hash_get_newbucket_from_oldbucket(rel, cur_bucket, lowmask, maxbucket);

    // Scan each page in the bucket chain
    for (;;) {
        HashPageOpaque opaque;
        OffsetNumber maxoffno;
        Page page = BufferGetPage(buf);
        OffsetNumber deletable[MaxOffsetNumber];
        int ndeletable = 0;
        bool retain_pin, clear_dead_marking = false;

        vacuum_delay_point();

        opaque = HashPageGetOpaque(page);
        maxoffno = PageGetMaxOffsetNumber(page);

        // Scan each tuple and mark for deletion if needed
        for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno++) {
            IndexTuple itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offno));
            ItemPointer htup = &(itup->t_tid);
            bool kill_tuple = false;

            // Check if tuple should be deleted via callback
            if (callback && callback(htup, callback_state)) {
                kill_tuple = true;
                if (tuples_removed) *tuples_removed += 1;
            }
            // Check if tuple was moved by split and should be cleaned up
            else if (split_cleanup) {
                Bucket bucket = _hash_hashkey2bucket(_hash_get_indextuple_hashkey(itup),
                                                   maxbucket, highmask, lowmask);
                if (bucket != cur_bucket) {
                    Assert(bucket == new_bucket);
                    kill_tuple = true;
                }
            }

            if (kill_tuple)
                deletable[ndeletable++] = offno;
            else if (num_index_tuples)
                *num_index_tuples += 1;
        }

        retain_pin = (blkno == bucket_blkno);
        blkno = opaque->hasho_nextblkno;

        // Apply deletions and WAL logging
        if (ndeletable > 0) {
            START_CRIT_SECTION();

            PageIndexMultiDelete(page, deletable, ndeletable);
            bucket_dirty = true;

            // Clear dead tuple marking if needed
            if (tuples_removed && *tuples_removed > 0 && H_HAS_DEAD_TUPLES(opaque)) {
                opaque->hasho_flag &= ~LH_PAGE_HAS_DEAD_TUPLES;
                clear_dead_marking = true;
            }

            MarkBufferDirty(buf);

            // WAL logging for deletions
            if (RelationNeedsWAL(rel)) {
                xl_hash_delete xlrec;
                xlrec.clear_dead_marking = clear_dead_marking;
                xlrec.is_primary_bucket_page = (buf == bucket_buf);

                XLogBeginInsert();
                XLogRegisterData((char *) &xlrec, SizeOfHashDelete);

                if (!xlrec.is_primary_bucket_page) {
                    uint8 flags = REGBUF_STANDARD | REGBUF_NO_IMAGE | REGBUF_NO_CHANGE;
                    XLogRegisterBuffer(0, bucket_buf, flags);
                }

                XLogRegisterBuffer(1, buf, REGBUF_STANDARD);
                XLogRegisterBufData(1, (char *) deletable, ndeletable * sizeof(OffsetNumber));
                XLogInsert(RM_HASH_ID, XLOG_HASH_DELETE);
            }

            END_CRIT_SECTION();
        }

        // Move to next page if exists
        if (!BlockNumberIsValid(blkno)) break;

        Buffer next_buf = _hash_getbuf_with_strategy(rel, blkno, HASH_WRITE,
                                                    LH_OVERFLOW_PAGE, bstrategy);
        if (retain_pin)
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        else
            _hash_relbuf(rel, buf);

        buf = next_buf;
    }

    // Clean up and finalize
    if (buf != bucket_buf) {
        _hash_relbuf(rel, buf);
        LockBuffer(bucket_buf, BUFFER_LOCK_EXCLUSIVE);
    }

    // Clear split cleanup flag
    if (split_cleanup) {
        Page page = BufferGetPage(bucket_buf);
        HashPageOpaque bucket_opaque = HashPageGetOpaque(page);

        START_CRIT_SECTION();
        bucket_opaque->hasho_flag &= ~LH_BUCKET_NEEDS_SPLIT_CLEANUP;
        MarkBufferDirty(bucket_buf);

        if (RelationNeedsWAL(rel)) {
            XLogBeginInsert();
            XLogRegisterBuffer(0, bucket_buf, REGBUF_STANDARD);
            XLogInsert(RM_HASH_ID, XLOG_HASH_SPLIT_CLEANUP);
        }
        END_CRIT_SECTION();
    }

    // Try to squeeze bucket if deletions occurred
    if (bucket_dirty && IsBufferCleanupOK(bucket_buf))
        _hash_squeezebucket(rel, cur_bucket, bucket_blkno, bucket_buf, bstrategy);
    else
        LockBuffer(bucket_buf, BUFFER_LOCK_UNLOCK);
}
```