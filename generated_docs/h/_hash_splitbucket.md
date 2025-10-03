# _hash_splitbucket

## Location
[src/backend/access/hash/hashpage.c:1073-1355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L1073-L1355)

## Overview
Partitions tuples between old and new buckets during hash table expansion, handling the core redistribution logic with support for incomplete split recovery.

## Definition
```c
static void _hash_splitbucket(Relation rel, Buffer metabuf, Bucket obucket, Bucket nbucket,
                             Buffer obuf, Buffer nbuf, HTAB *htab, uint32 maxbucket,
                             uint32 highmask, uint32 lowmask)
```

## Detailed Description
This function implements the core tuple redistribution algorithm for hash bucket splitting. It scans through all pages in the old bucket's overflow chain, determines which tuples belong in the new bucket based on their hash values, and moves appropriate tuples while marking them with INDEX_MOVED_BY_SPLIT_MASK. The function handles overflow page allocation for the new bucket when needed, supports recovery from incomplete splits via the htab parameter, and implements predicate lock copying for serializable isolation. After tuple redistribution, it updates bucket flags to mark the split as complete and optionally performs immediate cleanup of deleted tuples from the old bucket.

## Parameters / Member Variables
- `rel`: The hash index relation being split
- `metabuf`: Buffer containing metadata page (pinned, no lock required)
- `obucket`: Old bucket number being split
- `nbucket`: New bucket number receiving redistributed tuples
- `obuf`: Buffer for old bucket's primary page (cleanup lock required)
- `nbuf`: Buffer for new bucket's primary page (write lock, will be released)
- `htab`: Hash table of TIDs for incomplete split recovery (NULL for complete redistribution)
- `maxbucket`: Maximum bucket number for hash calculation
- `highmask`: High mask for hash-to-bucket mapping
- `lowmask`: Low mask for hash-to-bucket mapping

## Dependencies
- Functions called/Symbols referenced:
  - [PredicateLockPageSplit](../P/PredicateLockPageSplit.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - ItemIdIsDead
  - [PageGetItem](../P/PageGetItem.md)
  - [hash_search](hash_search.md)
  - [_hash_hashkey2bucket](_hash_hashkey2bucket.md)
  - [_hash_get_indextuple_hashkey](_hash_get_indextuple_hashkey.md)
  - [CopyIndexTuple](../C/CopyIndexTuple.md)
  - IndexTupleSize
  - [PageGetFreeSpaceForMultipleTuples](../P/PageGetFreeSpaceForMultipleTuples.md)
  - [_hash_pgaddmultitup](_hash_pgaddmultitup.md)
  - [_hash_addovflpage](_hash_addovflpage.md)
  - [_hash_getbuf](_hash_getbuf.md)
  - [hashbucketcleanup](hashbucketcleanup.md)
  - Various WAL logging functions
- Called from (representative examples):
  - [_hash_expandtable](_hash_expandtable.md)
  - [_hash_finish_split](_hash_finish_split.md)

## Notes and Other Information
- Requires cleanup locks on both old and new buckets to prevent concurrent access
- Implements batch tuple insertion for efficiency and reduced WAL overhead
- Marks moved tuples with INDEX_MOVED_BY_SPLIT_MASK to support concurrent scans
- Supports recovery from incomplete splits via selective tuple skipping using htab
- Handles overflow page allocation automatically when new bucket fills up
- Implements proper locking order (old bucket first, then new bucket) to avoid deadlocks
- Performs immediate cleanup of old bucket if possible to reduce bloat
- Uses critical sections around shared buffer modifications for crash safety
- Copies predicate locks to maintain serializable isolation level correctness

## Simplified Source

```c
static void _hash_splitbucket(Relation rel, Buffer metabuf, Bucket obucket, Bucket nbucket,
                             Buffer obuf, Buffer nbuf, HTAB *htab,
                             uint32 maxbucket, uint32 highmask, uint32 lowmask)
{
    Buffer bucket_obuf = obuf, bucket_nbuf = nbuf;
    Page opage, npage;
    HashPageOpaque oopaque, nopaque;
    OffsetNumber itup_offsets[MaxIndexTuplesPerPage];
    IndexTuple itups[MaxIndexTuplesPerPage];
    Size all_tups_size = 0;
    uint16 nitups = 0;

    opage = BufferGetPage(obuf);
    oopaque = HashPageGetOpaque(opage);
    npage = BufferGetPage(nbuf);
    nopaque = HashPageGetOpaque(npage);

    // Copy predicate locks from old bucket to new bucket
    PredicateLockPageSplit(rel, BufferGetBlockNumber(bucket_obuf), BufferGetBlockNumber(bucket_nbuf));

    // Main loop: process each page in old bucket's overflow chain
    for (;;) {
        OffsetNumber omaxoffnum = PageGetMaxOffsetNumber(opage);

        // Scan each tuple in current page
        for (OffsetNumber ooffnum = FirstOffsetNumber; ooffnum <= omaxoffnum; ooffnum = OffsetNumberNext(ooffnum)) {
            IndexTuple itup;
            Size itemsz;
            Bucket bucket;
            bool found = false;

            // Skip dead tuples
            if (ItemIdIsDead(PageGetItemId(opage, ooffnum)))
                continue;

            itup = (IndexTuple) PageGetItem(opage, PageGetItemId(opage, ooffnum));

            // Check if tuple should be skipped (for incomplete split recovery)
            if (htab)
                (void) hash_search(htab, &itup->t_tid, HASH_FIND, &found);
            if (found)
                continue;

            // Determine which bucket this tuple belongs to
            bucket = _hash_hashkey2bucket(_hash_get_indextuple_hashkey(itup),
                                         maxbucket, highmask, lowmask);

            if (bucket == nbucket) {
                // Tuple belongs in new bucket - copy and mark as moved
                IndexTuple new_itup = CopyIndexTuple(itup);
                new_itup->t_info |= INDEX_MOVED_BY_SPLIT_MASK;

                itemsz = MAXALIGN(IndexTupleSize(new_itup));

                // Check if we need a new overflow page
                if (PageGetFreeSpaceForMultipleTuples(npage, nitups + 1) < (all_tups_size + itemsz)) {
                    START_CRIT_SECTION();

                    // Add accumulated tuples to current page
                    _hash_pgaddmultitup(rel, nbuf, itups, itup_offsets, nitups);
                    MarkBufferDirty(nbuf);
                    log_split_page(rel, nbuf);

                    END_CRIT_SECTION();

                    LockBuffer(nbuf, BUFFER_LOCK_UNLOCK);

                    // Clean up tuple copies
                    for (int i = 0; i < nitups; i++)
                        pfree(itups[i]);
                    nitups = 0;
                    all_tups_size = 0;

                    // Allocate new overflow page
                    nbuf = _hash_addovflpage(rel, metabuf, nbuf, (nbuf == bucket_nbuf));
                    npage = BufferGetPage(nbuf);
                    nopaque = HashPageGetOpaque(npage);
                }

                // Add tuple to batch
                itups[nitups++] = new_itup;
                all_tups_size += itemsz;
            }
            // Tuple stays in old bucket - no action needed
        }

        BlockNumber oblkno = oopaque->hasho_nextblkno;

        // Release current page
        if (obuf == bucket_obuf)
            LockBuffer(obuf, BUFFER_LOCK_UNLOCK);
        else
            _hash_relbuf(rel, obuf);

        // Check if we've processed all pages
        if (!BlockNumberIsValid(oblkno)) {
            START_CRIT_SECTION();

            // Add final batch of tuples
            _hash_pgaddmultitup(rel, nbuf, itups, itup_offsets, nitups);
            MarkBufferDirty(nbuf);
            log_split_page(rel, nbuf);

            END_CRIT_SECTION();

            if (nbuf == bucket_nbuf)
                LockBuffer(nbuf, BUFFER_LOCK_UNLOCK);
            else
                _hash_relbuf(rel, nbuf);

            // Clean up
            for (int i = 0; i < nitups; i++)
                pfree(itups[i]);
            break;
        }

        // Move to next page in overflow chain
        obuf = _hash_getbuf(rel, oblkno, HASH_READ, LH_OVERFLOW_PAGE);
        opage = BufferGetPage(obuf);
        oopaque = HashPageGetOpaque(opage);
    }

    // Mark split as complete (proper locking order: old bucket first)
    LockBuffer(bucket_obuf, BUFFER_LOCK_EXCLUSIVE);
    opage = BufferGetPage(bucket_obuf);
    oopaque = HashPageGetOpaque(opage);

    LockBuffer(bucket_nbuf, BUFFER_LOCK_EXCLUSIVE);
    npage = BufferGetPage(bucket_nbuf);
    nopaque = HashPageGetOpaque(npage);

    START_CRIT_SECTION();

    // Update bucket flags
    oopaque->hasho_flag &= ~LH_BUCKET_BEING_SPLIT;
    nopaque->hasho_flag &= ~LH_BUCKET_BEING_POPULATED;
    oopaque->hasho_flag |= LH_BUCKET_NEEDS_SPLIT_CLEANUP;

    MarkBufferDirty(bucket_obuf);
    MarkBufferDirty(bucket_nbuf);

    // WAL logging
    if (RelationNeedsWAL(rel)) {
        xl_hash_split_complete xlrec;
        xlrec.old_bucket_flag = oopaque->hasho_flag;
        xlrec.new_bucket_flag = nopaque->hasho_flag;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfHashSplitComplete);
        XLogRegisterBuffer(0, bucket_obuf, REGBUF_STANDARD);
        XLogRegisterBuffer(1, bucket_nbuf, REGBUF_STANDARD);
        XLogRecPtr recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_SPLIT_COMPLETE);

        PageSetLSN(BufferGetPage(bucket_obuf), recptr);
        PageSetLSN(BufferGetPage(bucket_nbuf), recptr);
    }

    END_CRIT_SECTION();

    // Attempt immediate cleanup of old bucket if possible
    if (IsBufferCleanupOK(bucket_obuf)) {
        LockBuffer(bucket_nbuf, BUFFER_LOCK_UNLOCK);
        hashbucketcleanup(rel, obucket, bucket_obuf, BufferGetBlockNumber(bucket_obuf), NULL,
                         maxbucket, highmask, lowmask, NULL, NULL, true, NULL, NULL);
    } else {
        LockBuffer(bucket_nbuf, BUFFER_LOCK_UNLOCK);
        LockBuffer(bucket_obuf, BUFFER_LOCK_UNLOCK);
    }
}
```