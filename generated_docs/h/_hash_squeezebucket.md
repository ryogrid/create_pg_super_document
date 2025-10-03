# _hash_squeezebucket

## Location
[src/backend/access/hash/hashovfl.c:842-1125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashovfl.c#L842-L1125)

## Overview
Compacts tuples within a hash bucket chain by moving tuples from later pages to earlier pages to maximize space utilization and free unnecessary overflow pages.

## Definition

```c
void
_hash_squeezebucket(Relation rel,
					Bucket bucket,
					BlockNumber bucket_blkno,
					Buffer bucket_buf,
					BufferAccessStrategy bstrategy)
```
## Detailed Description
This function implements bucket compaction for hash indexes during VACUUM operations. It uses a two-pointer approach: a "write" pointer starting from the primary bucket page moving forward, and a "read" pointer starting from the last overflow page moving backward. The algorithm moves tuples from the read pages to fill available space in write pages, thereby eliminating empty or underutilized overflow pages.

The function maintains hashkey ordering when inserting moved tuples and uses WAL logging for crash safety. It employs lock chaining to prevent concurrent scans from seeing inconsistent bucket states during the reorganization process. All pages in the bucket chain are guaranteed to be non-empty after completion, unless the entire bucket is empty.

## Parameters / Member Variables
- : Relation (hash index) being processed
- : Bucket number being compacted  
- : Block number of the primary bucket page
- : Buffer containing the primary bucket page (must be cleanup-locked)
- : Buffer access strategy for controlling page fetches during VACUUM

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md), HashPageGetOpaque
  - BlockNumberIsValid, LockBuffer  
  - [_hash_relbuf](_hash_relbuf.md), _hash_getbuf_with_strategy
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md), PageGetItem, PageGetItemId
  - ItemIdIsDead, IndexTupleSize
  - [PageGetFreeSpaceForMultipleTuples](../P/PageGetFreeSpaceForMultipleTuples.md)
  - [_hash_pgaddmultitup](_hash_pgaddmultitup.md), PageIndexMultiDelete
  - [CopyIndexTuple](../C/CopyIndexTuple.md), _hash_freeovflpage
  - WAL functions: XLogEnsureRecordSpace, XLogBeginInsert, XLogRegisterData, XLogInsert
- Types/Constants referenced:
  - HashPageOpaque, IndexTuple, OffsetNumber
  - MaxOffsetNumber, MaxIndexTuplesPerPage
  - HASH_WRITE, LH_OVERFLOW_PAGE
  - XLOG_HASH_MOVE_PAGE_CONTENTS
- Called from:
  - [hashbucketcleanup](hashbucketcleanup.md) (primary caller during VACUUM)

## Notes and Other Information
- Requires cleanup lock on primary bucket page to exclude concurrent scans
- Uses lock chaining technique to prevent scan-squeeze interference  
- Preserves hashkey ordering when moving tuples between pages
- Handles WAL logging for multi-tuple operations with XLogEnsureRecordSpace
- Supports buffer access strategy to control memory usage during VACUUM
- Empty overflow pages encountered during the process are automatically freed
- The algorithm terminates when read and write pointers meet at the same page
- Critical sections protect the tuple movement operations for crash safety

## Simplified Source

```c
void _hash_squeezebucket(Relation rel, Bucket bucket, BlockNumber bucket_blkno,
                        Buffer bucket_buf, BufferAccessStrategy bstrategy) {
    Buffer wbuf = bucket_buf;  // write buffer (starts at primary page)
    Buffer rbuf;               // read buffer (starts at last page)
    Page wpage, rpage;
    HashPageOpaque wopaque, ropaque;

    // Setup write pointer at primary bucket page
    wpage = BufferGetPage(wbuf);
    wopaque = HashPageGetOpaque(wpage);

    // Exit if no overflow pages exist
    if (!BlockNumberIsValid(wopaque->hasho_nextblkno)) {
        LockBuffer(wbuf, BUFFER_LOCK_UNLOCK);
        return;
    }

    // Find last page in bucket chain for read pointer
    rbuf = wbuf;
    ropaque = wopaque;
    do {
        BlockNumber next_blkno = ropaque->hasho_nextblkno;
        if (rbuf != wbuf) _hash_relbuf(rel, rbuf);
        rbuf = _hash_getbuf_with_strategy(rel, next_blkno, HASH_WRITE,
                                         LH_OVERFLOW_PAGE, bstrategy);
        rpage = BufferGetPage(rbuf);
        ropaque = HashPageGetOpaque(rpage);
    } while (BlockNumberIsValid(ropaque->hasho_nextblkno));

    // Main squeeze loop: move tuples from read pages to write pages
    for (;;) {
        IndexTuple itups[MaxIndexTuplesPerPage];
        OffsetNumber deletable[MaxOffsetNumber];
        Size tups_size[MaxIndexTuplesPerPage];
        uint16 nitups = 0, ndeletable = 0;
        Size all_tups_size = 0;

        // Collect live tuples from current read page
        OffsetNumber maxroffnum = PageGetMaxOffsetNumber(rpage);
        for (OffsetNumber roffnum = FirstOffsetNumber; roffnum <= maxroffnum; roffnum++) {
            if (ItemIdIsDead(PageGetItemId(rpage, roffnum))) continue;

            IndexTuple itup = (IndexTuple) PageGetItem(rpage, PageGetItemId(rpage, roffnum));
            Size itemsz = MAXALIGN(IndexTupleSize(itup));

            // Find write page with enough space
            while (PageGetFreeSpaceForMultipleTuples(wpage, nitups + 1) < (all_tups_size + itemsz)) {
                // Move accumulated tuples to current write page
                if (nitups > 0) {
                    _hash_pgaddmultitup(rel, wbuf, itups, itup_offsets, nitups);
                    PageIndexMultiDelete(rpage, deletable, ndeletable);
                    // WAL logging for tuple movement
                    if (RelationNeedsWAL(rel)) {
                        // [WAL logging code simplified...]
                    }
                }

                // Advance to next write page
                wbuf = _hash_getbuf_with_strategy(rel, wopaque->hasho_nextblkno,
                                                 HASH_WRITE, LH_OVERFLOW_PAGE, bstrategy);
                wpage = BufferGetPage(wbuf);
                wopaque = HashPageGetOpaque(wpage);

                // Reset tuple collection for next page
                nitups = ndeletable = 0;
                all_tups_size = 0;
            }

            // Add tuple to collection for moving
            deletable[ndeletable++] = roffnum;
            itups[nitups] = CopyIndexTuple(itup);
            tups_size[nitups++] = itemsz;
            all_tups_size += itemsz;
        }

        // Free empty read page and move to previous page
        BlockNumber prev_rblkno = ropaque->hasho_prevblkno;
        _hash_freeovflpage(rel, bucket_buf, rbuf, wbuf, itups, itup_offsets,
                          tups_size, nitups, bstrategy);

        // Check if squeeze is complete
        if (prev_rblkno == wblkno) return;

        // Move to previous read page
        rbuf = _hash_getbuf_with_strategy(rel, prev_rblkno, HASH_WRITE,
                                         LH_OVERFLOW_PAGE, bstrategy);
        rpage = BufferGetPage(rbuf);
        ropaque = HashPageGetOpaque(rpage);
    }
}
```