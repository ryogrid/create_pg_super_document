# _hash_kill_items

## Location
[src/backend/access/hash/hashutil.c:536-621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashutil.c#L536-L621)

## Overview
Sets LP_DEAD state for index tuples that an index scan caller has identified as killed, marking them as dead hints for future cleanup operations.

## Definition

```c
void
_hash_kill_items(IndexScanDesc scan)
```
## Detailed Description
The  function is responsible for marking index tuples as dead (LP_DEAD state) based on information provided by an index scan caller. This function operates as part of PostgreSQL's hash index implementation and serves as an optimization mechanism for tuple cleanup.

The function works by:
1. Accessing the scan's opaque data structure which contains information about killed tuples
2. Locating the appropriate page buffer (either already pinned or requiring acquisition)
3. Iterating through the list of killed items and matching them by heap TID
4. Marking matching tuples as dead using ItemIdMarkDead()
5. Setting the page's LH_PAGE_HAS_DEAD_TUPLES flag as a hint for future cleanup
6. Properly managing buffer locks and pins

The function includes important concurrency considerations: it only requires a read lock to set LP_DEAD status since this is just a hint, and it accounts for VACUUM operations that may be running concurrently on the same bucket.

## Parameters
- : IndexScanDesc containing the index scan descriptor with information about killed tuples stored in scan->opaque

## Dependencies
- Functions called/Symbols referenced:
  - HashScanPosIsValid
  - HashScanPosIsPinned
  - [LockBuffer](../L/LockBuffer.md)
  - [_hash_getbuf](_hash_getbuf.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - HashPageGetOpaque
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
  - ItemIdMarkDead
  - OffsetNumberNext
  - [MarkBufferDirtyHint](../M/MarkBufferDirtyHint.md)
  - [_hash_relbuf](_hash_relbuf.md)

- Called from:
  - [hashrescan](hashrescan.md) (src/backend/access/hash/hash.c:407)
  - [hashendscan](hashendscan.md) (src/backend/access/hash/hash.c:440)
  - [_hash_next](_hash_next.md) (src/backend/access/hash/hashsearch.c:68, 86)
  - [_hash_readpage](_hash_readpage.md) (src/backend/access/hash/hashsearch.c:485, 544)

## Simplified Source
```c
void _hash_kill_items(IndexScanDesc scan) {
    HashScanOpaque so = (HashScanOpaque) scan->opaque;
    Relation rel = scan->indexRelation;
    int numKilled = so->numKilled;
    bool killedsomething = false;

    // Reset scan state
    so->numKilled = 0;

    // Get the page buffer (may already be pinned)
    Buffer buf;
    if (HashScanPosIsPinned(so->currPos)) {
        buf = so->currPos.buf;
        LockBuffer(buf, BUFFER_LOCK_SHARE);
    } else {
        buf = _hash_getbuf(rel, so->currPos.currPage, HASH_READ, LH_OVERFLOW_PAGE);
    }

    Page page = BufferGetPage(buf);
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

    // Mark each killed item as dead by matching heap TIDs
    for (int i = 0; i < numKilled; i++) {
        HashScanPosItem *currItem = &so->currPos.items[so->killedItems[i]];

        for (OffsetNumber offnum = currItem->indexOffset; offnum <= maxoff; offnum++) {
            ItemId iid = PageGetItemId(page, offnum);
            IndexTuple ituple = (IndexTuple) PageGetItem(page, iid);

            if (ItemPointerEquals(&ituple->t_tid, &currItem->heapTid)) {
                ItemIdMarkDead(iid);
                killedsomething = true;
                break;
            }
        }
    }

    // Mark page as having dead tuples if any were killed
    if (killedsomething) {
        HashPageGetOpaque(page)->hasho_flag |= LH_PAGE_HAS_DEAD_TUPLES;
        MarkBufferDirtyHint(buf, true);
    }

    // Release buffer lock/pin appropriately
    if (so->hashso_bucket_buf == so->currPos.buf || HashScanPosIsPinned(so->currPos))
        LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
    else
        _hash_relbuf(rel, buf);
}
```

## Notes and Other Information
- The function assumes so->numKilled > 0 and resets this counter to 0 after processing
- Buffer management is carefully handled - the function may or may not have the page pinned initially
- Only a read lock (BUFFER_LOCK_SHARE) is required since LP_DEAD is just a hint
- The function matches items by heap TID before marking them dead to ensure correctness
- Includes important concurrency safety: VACUUM operations are coordinated through proper locking to prevent accidental killing of reused TIDs
- When items are killed, both individual tuples are marked dead and the page gets the LH_PAGE_HAS_DEAD_TUPLES flag
- Uses MarkBufferDirtyHint() since the changes are hints that can be redone if needed