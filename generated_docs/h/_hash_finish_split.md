# _hash_finish_split

## Location
[src/backend/access/hash/hashpage.c:1356-1473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L1356-L1473)

## Overview
Completes a previously interrupted hash table bucket split operation by building a hash table of TIDs from the new bucket and performing the actual split.

## Definition

```c
void
_hash_finish_split(Relation rel, Buffer metabuf, Buffer obuf, Bucket obucket,
				   uint32 maxbucket, uint32 highmask, uint32 lowmask)
```
## Detailed Description
This function is responsible for completing a bucket split operation that was previously interrupted. It works by:

1. Creating an in-memory hash table to track TIDs (tuple identifiers) that have already been moved to the new bucket
2. Scanning the new bucket and all its overflow pages to populate this TID hash table
3. Attempting to acquire cleanup locks on both old and new buckets
4. If locks are successfully acquired, calling  to complete the split operation using the TID hash table to skip already-moved tuples

The function handles the case where a split operation was interrupted (e.g., due to a crash) and needs to be completed. The TID hash table ensures that tuples already moved to the new bucket are not processed again during the completion of the split.

## Parameters / Member Variables
- `rel`: The hash index relation being operated on
- `metabuf`: Buffer containing the metapage (must be pinned but not locked)
- `obuf`: Buffer containing the old bucket's primary page (must be pinned but not locked)
- `obucket`: The bucket number of the old bucket being split
- `maxbucket`: The current maximum bucket number in the hash table
- `highmask`: High-order bits mask for hash value calculation
- `lowmask`: Low-order bits mask for hash value calculation
## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](hash_create.md) (creates the TID tracking hash table)
  - [_hash_get_newblock_from_oldbucket](_hash_get_newblock_from_oldbucket.md) (gets the new bucket's block number)
  - [_hash_getbuf](_hash_getbuf.md) (reads bucket pages)
  - [hash_search](hash_search.md) (inserts TIDs into the tracking hash table)
  - [ConditionalLockBufferForCleanup](../C/ConditionalLockBufferForCleanup.md) (attempts to acquire cleanup locks)
  - [_hash_splitbucket](_hash_splitbucket.md) (performs the actual split operation)
  - [_hash_dropbuf](_hash_dropbuf.md) (releases buffer)
  - [hash_destroy](hash_destroy.md) (cleans up the TID hash table)
- Called from (representative examples):
  - [_hash_doinsert](_hash_doinsert.md) (during tuple insertion when split completion is needed)
  - [_hash_expandtable](_hash_expandtable.md) (during table expansion operations)

## Notes and Other Information
- The function uses conditional locking to avoid blocking - if cleanup locks cannot be acquired, it silently gives up and the split will be retried on the next insertion
- The TID hash table is created in the current memory context and is destroyed before the function returns
- The function maintains pins on the metapage and old bucket buffers as required by the caller
- This is part of PostgreSQL's hash index implementation for handling interrupted split operations gracefully
- The function handles both the primary bucket page and any overflow pages in the new bucket

## Simplified Source

```c
void _hash_finish_split(Relation rel, Buffer metabuf, Buffer obuf, Bucket obucket,
                       uint32 maxbucket, uint32 highmask, uint32 lowmask) {
    // Create hash table to track TIDs already moved to new bucket
    HASHCTL hash_ctl;
    hash_ctl.keysize = sizeof(ItemPointerData);
    hash_ctl.entrysize = sizeof(ItemPointerData);
    hash_ctl.hcxt = CurrentMemoryContext;

    HTAB *tidhtab = hash_create("bucket ctids", 256, &hash_ctl,
                               HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    // Get new bucket's block number
    BlockNumber bucket_nblkno = _hash_get_newblock_from_oldbucket(rel, obucket);
    BlockNumber nblkno = bucket_nblkno;
    Buffer bucket_nbuf = InvalidBuffer;

    // Scan new bucket and all overflow pages to build TID hash table
    for (;;) {
        Buffer nbuf = _hash_getbuf(rel, nblkno, HASH_READ,
                                  LH_BUCKET_PAGE | LH_OVERFLOW_PAGE);

        // Remember primary bucket buffer for cleanup lock
        if (nblkno == bucket_nblkno)
            bucket_nbuf = nbuf;

        Page npage = BufferGetPage(nbuf);
        HashPageOpaque npageopaque = HashPageGetOpaque(npage);

        // Add all TIDs from this page to hash table
        OffsetNumber nmaxoffnum = PageGetMaxOffsetNumber(npage);
        for (OffsetNumber noffnum = FirstOffsetNumber;
             noffnum <= nmaxoffnum;
             noffnum = OffsetNumberNext(noffnum)) {

            IndexTuple itup = (IndexTuple) PageGetItem(npage,
                                     PageGetItemId(npage, noffnum));
            bool found;
            hash_search(tidhtab, &itup->t_tid, HASH_ENTER, &found);
        }

        nblkno = npageopaque->hasho_nextblkno;

        // Release buffer (keep pin on primary bucket)
        if (nbuf == bucket_nbuf)
            LockBuffer(nbuf, BUFFER_LOCK_UNLOCK);
        else
            _hash_relbuf(rel, nbuf);

        // Exit if no more overflow pages
        if (!BlockNumberIsValid(nblkno))
            break;
    }

    // Try to get cleanup locks on both buckets
    if (!ConditionalLockBufferForCleanup(obuf)) {
        hash_destroy(tidhtab);
        return;
    }
    if (!ConditionalLockBufferForCleanup(bucket_nbuf)) {
        LockBuffer(obuf, BUFFER_LOCK_UNLOCK);
        hash_destroy(tidhtab);
        return;
    }

    // Complete the split operation using TID hash table
    Page npage = BufferGetPage(bucket_nbuf);
    HashPageOpaque npageopaque = HashPageGetOpaque(npage);
    Bucket nbucket = npageopaque->hasho_bucket;

    _hash_splitbucket(rel, metabuf, obucket, nbucket, obuf, bucket_nbuf,
                     tidhtab, maxbucket, highmask, lowmask);

    // Cleanup
    _hash_dropbuf(rel, bucket_nbuf);
    hash_destroy(tidhtab);
}
```