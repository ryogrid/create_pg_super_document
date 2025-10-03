# _hash_readnext

## Location
[src/backend/access/hash/hashsearch.c:131-196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashsearch.c#L131-L196)

## Overview
Advances to the next page in a hash bucket during scanning, with special handling for bucket splits and buffer management.

## Definition
```c
static void _hash_readnext(IndexScanDesc scan, Buffer *bufp, Page *pagep, HashPageOpaque *opaquep)
```

## Detailed Description
This function moves to the next page in a hash bucket chain during index scanning. It handles the complex case where a bucket split is occurring, allowing the scan to transition from the populated bucket to the bucket being split. The function manages buffer locks and pins carefully, maintaining pins on primary bucket pages throughout the scan while properly releasing overflow page buffers.

When reaching the end of a bucket during a split operation, the function transitions to scanning the split bucket if one exists. It also implements proper interrupt checking and predicate locking for MVCC compliance.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing the scan state and relation information
- `bufp`: Pointer to Buffer being updated to point to the next page
- `pagep`: Pointer to Page being updated to point to the next page content
- `opaquep`: Pointer to HashPageOpaque being updated with next page's opaque data

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumberIsValid
  - [_hash_getbuf](_hash_getbuf.md)
  - [_hash_relbuf](_hash_relbuf.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [PredicateLockPage](../P/PredicateLockPage.md)
  - HashPageGetOpaque
  - CHECK_FOR_INTERRUPTS
- Called from (representative examples):
  - [_hash_readprev](_hash_readprev.md)
  - [_hash_first](_hash_first.md)
  - [_hash_readpage](_hash_readpage.md)

## Notes and Other Information
The function maintains pins on primary bucket pages throughout the scan operation for efficiency. During bucket splits, it handles the transition from the populated bucket to the split bucket seamlessly. Interrupt checking is performed while no buffer locks are held to ensure responsiveness. The hashso_buc_split flag tracks whether the scan has transitioned to scanning the split bucket.

## Simplified Source

```c
static void
_hash_readnext(IndexScanDesc scan,
               Buffer *bufp, Page *pagep, HashPageOpaque *opaquep)
{
    BlockNumber blkno;
    Relation rel = scan->indexRelation;
    HashScanOpaque so = (HashScanOpaque) scan->opaque;
    bool block_found = false;

    blkno = (*opaquep)->hasho_nextblkno;

    // Release current buffer but keep pin on bucket pages
    if (*bufp == so->hashso_bucket_buf || *bufp == so->hashso_split_bucket_buf)
        LockBuffer(*bufp, BUFFER_LOCK_UNLOCK);
    else
        _hash_relbuf(rel, *bufp);

    *bufp = InvalidBuffer;
    CHECK_FOR_INTERRUPTS();

    // Try to get next overflow page
    if (BlockNumberIsValid(blkno))
    {
        *bufp = _hash_getbuf(rel, blkno, HASH_READ, LH_OVERFLOW_PAGE);
        block_found = true;
    }
    // Handle bucket split case - transition to split bucket
    else if (so->hashso_buc_populated && !so->hashso_buc_split)
    {
        // End of populated bucket - switch to split bucket
        *bufp = so->hashso_split_bucket_buf;

        LockBuffer(*bufp, BUFFER_LOCK_SHARE);
        PredicateLockPage(rel, BufferGetBlockNumber(*bufp), scan->xs_snapshot);

        // Mark that we're now scanning the split bucket
        so->hashso_buc_split = true;
        block_found = true;
    }

    // Update page pointers if we found a valid block
    if (block_found)
    {
        *pagep = BufferGetPage(*bufp);
        *opaquep = HashPageGetOpaque(*pagep);
    }
}
```