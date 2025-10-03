# _hash_readpage

## Location
[src/backend/access/hash/hashsearch.c:446-601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashsearch.c#L446-L601)

## Overview
Loads qualifying index tuples from the current hash index page into the scan position, handling page navigation when no matches are found.

## Definition
```c
static bool _hash_readpage(IndexScanDesc scan, Buffer *bufP, ScanDirection dir)
```

## Detailed Description
This function scans the current hash index page to find tuples that satisfy the scan qualification and loads them into the scan's current position structure. It uses binary search to locate the starting position on each page based on the hash key. When no qualifying tuples are found on the current page, it automatically advances to the next or previous page depending on scan direction.

The function handles both forward and backward scan directions with different positioning logic. For forward scans, it starts from the beginning of qualifying items and processes to the end. For backward scans, it starts from the end and processes backward. The function maintains proper buffer management, keeping pins on bucket pages while releasing overflow page buffers after loading their data.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing the scan state and relation information
- `bufP`: Pointer to Buffer being processed for tuple loading
- `dir`: ScanDirection indicating forward or backward scan direction

## Dependencies
- Functions called/Symbols referenced:
  - [_hash_checkpage](_hash_checkpage.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - HashPageGetOpaque
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - ScanDirectionIsForward
  - [_hash_binsearch](_hash_binsearch.md)
  - [_hash_binsearch_last](_hash_binsearch_last.md)
  - [_hash_load_qualified_items](_hash_load_qualified_items.md)
  - [_hash_kill_items](_hash_kill_items.md)
  - [_hash_readnext](_hash_readnext.md)
  - [_hash_readprev](_hash_readprev.md)
  - [_hash_relbuf](_hash_relbuf.md)
  - [LockBuffer](../L/LockBuffer.md)
- Called from (representative examples):
  - [_hash_next](_hash_next.md)
  - [_hash_first](_hash_first.md)

## Notes and Other Information
The function performs page validation using _hash_checkpage to ensure proper page type. Binary search positioning differs between scan directions: forward scans use _hash_binsearch while backward scans use _hash_binsearch_last. Buffer management maintains pins on bucket pages throughout scans but releases overflow page buffers after data extraction. The function handles scrollable cursor requirements by preserving page navigation information. Return value indicates whether qualifying tuples were found and loaded successfully.

## Simplified Source

```c
static bool
_hash_readpage(IndexScanDesc scan, Buffer *bufP, ScanDirection dir)
{
    Relation rel = scan->indexRelation;
    HashScanOpaque so = (HashScanOpaque) scan->opaque;
    Buffer buf;
    Page page;
    HashPageOpaque opaque;
    OffsetNumber offnum;
    uint16 itemIndex;

    buf = *bufP;
    _hash_checkpage(rel, buf, LH_BUCKET_PAGE | LH_OVERFLOW_PAGE);
    page = BufferGetPage(buf);
    opaque = HashPageGetOpaque(page);

    so->currPos.buf = buf;
    so->currPos.currPage = BufferGetBlockNumber(buf);

    if (ScanDirectionIsForward(dir))
    {
        BlockNumber prev_blkno = InvalidBlockNumber;

        for (;;)
        {
            // Find starting position using binary search
            offnum = _hash_binsearch(page, so->hashso_sk_hash);
            itemIndex = _hash_load_qualified_items(scan, page, offnum, dir);

            if (itemIndex != 0)
                break;

            // No matches on current page - kill items and move to next page
            if (so->numKilled > 0)
                _hash_kill_items(scan);

            // Track previous block for navigation
            if (so->currPos.buf == so->hashso_bucket_buf ||
                so->currPos.buf == so->hashso_split_bucket_buf)
                prev_blkno = InvalidBlockNumber;
            else
                prev_blkno = opaque->hasho_prevblkno;

            // Move to next page
            _hash_readnext(scan, &buf, &page, &opaque);
            if (BufferIsValid(buf))
            {
                so->currPos.buf = buf;
                so->currPos.currPage = BufferGetBlockNumber(buf);
            }
            else
            {
                // End of scan - preserve navigation info
                so->currPos.prevPage = prev_blkno;
                so->currPos.nextPage = InvalidBlockNumber;
                so->currPos.buf = buf;
                return false;
            }
        }

        // Set up forward scan position
        so->currPos.firstItem = 0;
        so->currPos.lastItem = itemIndex - 1;
        so->currPos.itemIndex = 0;
    }
    else
    {
        // Backward scan logic
        BlockNumber next_blkno = InvalidBlockNumber;

        for (;;)
        {
            // Find starting position for backward scan
            offnum = _hash_binsearch_last(page, so->hashso_sk_hash);
            itemIndex = _hash_load_qualified_items(scan, page, offnum, dir);

            if (itemIndex != MaxIndexTuplesPerPage)
                break;

            // No matches - move to previous page
            if (so->numKilled > 0)
                _hash_kill_items(scan);

            if (so->currPos.buf == so->hashso_bucket_buf ||
                so->currPos.buf == so->hashso_split_bucket_buf)
                next_blkno = opaque->hasho_nextblkno;

            _hash_readprev(scan, &buf, &page, &opaque);
            if (BufferIsValid(buf))
            {
                so->currPos.buf = buf;
                so->currPos.currPage = BufferGetBlockNumber(buf);
            }
            else
            {
                so->currPos.prevPage = InvalidBlockNumber;
                so->currPos.nextPage = next_blkno;
                so->currPos.buf = buf;
                return false;
            }
        }

        // Set up backward scan position
        so->currPos.firstItem = itemIndex;
        so->currPos.lastItem = MaxIndexTuplesPerPage - 1;
        so->currPos.itemIndex = MaxIndexTuplesPerPage - 1;
    }

    // Set navigation pointers and release locks
    if (so->currPos.buf == so->hashso_bucket_buf ||
        so->currPos.buf == so->hashso_split_bucket_buf)
    {
        so->currPos.prevPage = InvalidBlockNumber;
        so->currPos.nextPage = opaque->hasho_nextblkno;
        LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
    }
    else
    {
        so->currPos.prevPage = opaque->hasho_prevblkno;
        so->currPos.nextPage = opaque->hasho_nextblkno;
        _hash_relbuf(rel, so->currPos.buf);
        so->currPos.buf = InvalidBuffer;
    }

    return true;
}
```