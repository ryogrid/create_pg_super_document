# _hash_doinsert

## Location
[src/backend/access/hash/hashinsert.c:38-273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashinsert.c#L38-L273)

## Overview
The  function handles the insertion of a single index tuple into a hash index, including all necessary logic for bucket management, overflow pages, and potential table expansion.

## Definition

```c
void
_hash_doinsert(Relation rel, IndexTuple itup, Relation heapRel, bool sorted)
```
## Detailed Description
This is the core insertion function for PostgreSQL's hash index implementation. It performs a complete tuple insertion process including:

1. **Hash key computation and validation**: Extracts the hash key from the index tuple and validates that the tuple size doesn't exceed hash page limits.

2. **Bucket location and locking**: Locates the appropriate bucket page using the hash key and acquires write locks.

3. **Split completion handling**: If the target bucket is in the process of being split, completes the split operation first to potentially create space for the new tuple.

4. **Space management**: Searches through the bucket chain (primary page and overflow pages) to find sufficient space. If no space is available, it either:
   - Cleans up dead tuples on pages with cleanup locks
   - Allocates new overflow pages when needed

5. **Tuple insertion and metadata update**: Adds the tuple to the appropriate page, updates the global tuple count in the metapage, and determines if table expansion is needed.

6. **WAL logging**: Records the insertion operation for crash recovery when WAL is enabled.

7. **Table expansion**: Triggers hash table expansion if the load factor threshold is exceeded.

The function includes restart logic to handle cases where bucket splits occur during insertion, ensuring consistency and optimal space utilization.

## Parameters / Member Variables
- : The hash index relation being inserted into
- : The completely filled index tuple to be inserted
- : The heap relation (used for vacuum operations on dead tuples)
- : Boolean flag indicating if inserts are done in hashkey order (optimization hint)

## Dependencies
- Functions called/Symbols referenced:
  - : Extract hash key from tuple
  - : Locate and lock bucket page
  - : Complete bucket split operations
  - : Clean up dead tuples
  - : Allocate new overflow pages
  - : Add tuple to page
  - : Expand hash table when load factor exceeded
  - , : Size and space calculations
  - Various buffer management functions (, , )
  - WAL logging functions (, , etc.)

- Called from (representative examples):
  - : Public interface for single tuple insertion
  - : Bulk loading during index creation
  - : Hash index build process

## Notes and Other Information
- The function uses a restart mechanism ( label) to handle bucket splits that occur during insertion
- Dead tuple cleanup is performed opportunistically when cleanup locks are available
- The load factor check () determines when table expansion is needed
- Critical sections protect the actual tuple insertion and metadata updates to ensure atomicity
- Buffer management carefully distinguishes between primary bucket pages (pin retained) and overflow pages (pin released)
- The  parameter is an optimization hint for bulk loading scenarios where tuples arrive in hash key order

## Simplified Source

```c
void _hash_doinsert(Relation rel, IndexTuple itup, Relation heapRel, bool sorted) {
    Buffer buf, bucket_buf, metabuf;
    HashMetaPage metap, usedmetap = NULL;
    Page metapage, page;
    HashPageOpaque pageopaque;
    Size itemsz;
    bool do_expand;
    uint32 hashkey;
    Bucket bucket;
    OffsetNumber itup_off;

    // Get hash key from the index tuple
    hashkey = _hash_get_indextuple_hashkey(itup);

    // Calculate aligned item size
    itemsz = MAXALIGN(IndexTupleSize(itup));

restart_insert:
    // Read metapage and check if item fits
    metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_NOLOCK, LH_META_PAGE);
    metapage = BufferGetPage(metabuf);

    if (itemsz > HashMaxItemSize(metapage))
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       errmsg("index row size %zu exceeds hash maximum %zu",
                             itemsz, HashMaxItemSize(metapage))));

    // Lock the target bucket page
    buf = _hash_getbucketbuf_from_hashkey(rel, hashkey, HASH_WRITE, &usedmetap);
    bucket_buf = buf;
    page = BufferGetPage(buf);
    pageopaque = HashPageGetOpaque(page);
    bucket = pageopaque->hasho_bucket;

    // Complete bucket split if in progress
    if (H_BUCKET_BEING_SPLIT(pageopaque) && IsBufferCleanupOK(buf)) {
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        _hash_finish_split(rel, metabuf, buf, bucket,
                          usedmetap->hashm_maxbucket,
                          usedmetap->hashm_highmask,
                          usedmetap->hashm_lowmask);
        _hash_dropbuf(rel, buf);
        _hash_dropbuf(rel, metabuf);
        goto restart_insert;
    }

    // Find space for insertion - traverse bucket chain
    while (PageGetFreeSpace(page) < itemsz) {
        // Try to clean dead tuples first
        if (H_HAS_DEAD_TUPLES(pageopaque) && IsBufferCleanupOK(buf)) {
            _hash_vacuum_one_page(rel, heapRel, metabuf, buf);
            if (PageGetFreeSpace(page) >= itemsz)
                break;
        }

        // Move to next page in chain or create overflow page
        BlockNumber nextblkno = pageopaque->hasho_nextblkno;
        if (BlockNumberIsValid(nextblkno)) {
            // Move to existing overflow page
            if (buf != bucket_buf)
                _hash_relbuf(rel, buf);
            else
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            buf = _hash_getbuf(rel, nextblkno, HASH_WRITE, LH_OVERFLOW_PAGE);
            page = BufferGetPage(buf);
        } else {
            // Create new overflow page
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            buf = _hash_addovflpage(rel, metabuf, buf, (buf == bucket_buf));
            page = BufferGetPage(buf);
        }
        pageopaque = HashPageGetOpaque(page);
    }

    // Insert tuple and update metadata
    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
    START_CRIT_SECTION();

    // Add tuple to page
    itup_off = _hash_pgaddtup(rel, buf, itemsz, itup, sorted);
    MarkBufferDirty(buf);

    // Update tuple count and check for expansion
    metap = HashPageGetMeta(metapage);
    metap->hashm_ntuples += 1;
    do_expand = metap->hashm_ntuples >
                (double) metap->hashm_ffactor * (metap->hashm_maxbucket + 1);
    MarkBufferDirty(metabuf);

    // WAL logging
    if (RelationNeedsWAL(rel)) {
        xl_hash_insert xlrec;
        xlrec.offnum = itup_off;
        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfHashInsert);
        XLogRegisterBuffer(1, metabuf, REGBUF_STANDARD);
        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
        XLogRegisterBufData(0, (char *) itup, IndexTupleSize(itup));
        XLogRecPtr recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_INSERT);
        PageSetLSN(BufferGetPage(buf), recptr);
        PageSetLSN(BufferGetPage(metabuf), recptr);
    }

    END_CRIT_SECTION();

    // Release buffers and expand table if needed
    LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
    _hash_relbuf(rel, buf);
    if (buf != bucket_buf)
        _hash_dropbuf(rel, bucket_buf);

    if (do_expand)
        _hash_expandtable(rel, metabuf);

    _hash_dropbuf(rel, metabuf);
}
```