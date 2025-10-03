# _hash_first

## Location
[src/backend/access/hash/hashsearch.c:288-445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashsearch.c#L288-L445)

## Overview
Initiates a hash index scan by finding the first (or last for backward scans) qualifying tuple in the appropriate bucket.

## Definition
```c
bool _hash_first(IndexScanDesc scan, ScanDirection dir)
```

## Detailed Description
This function starts a hash index scan by computing the hash key from the scan qualification, locating the appropriate bucket, and finding the first qualifying tuple. It handles complex scenarios including bucket splits in progress, where special buffer management is required to maintain consistency. The function validates that hash scans have proper qualifications (whole-index scans are not supported) and implements proper locking protocols when bucket splits are occurring.

For backward scans, the function positions at the end of the bucket chain. When a bucket split is detected, it maintains pins on both the populated bucket and the split bucket to ensure vacuum operations don't interfere with the scan. The function also handles cross-type comparisons by choosing the appropriate hash function.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing scan parameters and state information
- `dir`: ScanDirection indicating forward or backward scan direction

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_count_index_scan
  - [_hash_datum2hashkey](_hash_datum2hashkey.md)
  - [_hash_datum2hashkey_type](_hash_datum2hashkey_type.md)
  - [_hash_getbucketbuf_from_hashkey](_hash_getbucketbuf_from_hashkey.md)
  - [PredicateLockPage](../P/PredicateLockPage.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - HashPageGetOpaque
  - H_BUCKET_BEING_POPULATED
  - [_hash_get_oldblock_from_newbucket](_hash_get_oldblock_from_newbucket.md)
  - [_hash_getbuf](_hash_getbuf.md)
  - [_hash_dropbuf](_hash_dropbuf.md)
  - ScanDirectionIsBackward
  - BlockNumberIsValid
  - [_hash_readnext](_hash_readnext.md)
  - [_hash_readpage](_hash_readpage.md)
- Called from (representative examples):
  - [hashgettuple](hashgettuple.md)
  - [hashgetbitmap](hashgetbitmap.md)

## Notes and Other Information
The function requires at least one scan key and only supports equality operations (HTEqualStrategyNumber). NULL values in scan keys cause immediate failure since hash indexes cannot match NULL values. During bucket splits, careful locking order is maintained: acquire lock on split bucket first, then release lock but keep pin, then acquire lock on populated bucket. This prevents deadlocks with vacuum operations. The function maintains statistics by calling pgstat_count_index_scan and implements predicate locking for snapshot isolation.

## Simplified Source

```c
bool
_hash_first(IndexScanDesc scan, ScanDirection dir)
{
    Relation rel = scan->indexRelation;
    HashScanOpaque so = (HashScanOpaque) scan->opaque;
    ScanKey cur;
    uint32 hashkey;
    Bucket bucket;
    Buffer buf;
    Page page;
    HashPageOpaque opaque;
    HashScanPosItem *currItem;

    pgstat_count_index_scan(rel);

    // Hash indexes require qualification and don't support whole-index scans
    if (scan->numberOfKeys < 1)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("hash indexes do not support whole-index scans")));

    cur = &scan->keyData[0];

    // NULL values cannot match in hash indexes
    if (cur->sk_flags & SK_ISNULL)
        return false;

    // Compute hash key for the search value
    if (cur->sk_subtype == rel->rd_opcintype[0] ||
        cur->sk_subtype == InvalidOid)
        hashkey = _hash_datum2hashkey(rel, cur->sk_argument);
    else
        hashkey = _hash_datum2hashkey_type(rel, cur->sk_argument,
                                           cur->sk_subtype);

    so->hashso_sk_hash = hashkey;

    // Get the appropriate bucket buffer
    buf = _hash_getbucketbuf_from_hashkey(rel, hashkey, HASH_READ, NULL);
    PredicateLockPage(rel, BufferGetBlockNumber(buf), scan->xs_snapshot);
    page = BufferGetPage(buf);
    opaque = HashPageGetOpaque(page);
    bucket = opaque->hasho_bucket;

    so->hashso_bucket_buf = buf;

    // Handle bucket split scenario
    if (H_BUCKET_BEING_POPULATED(opaque))
    {
        BlockNumber old_blkno;
        Buffer old_buf;

        old_blkno = _hash_get_oldblock_from_newbucket(rel, bucket);

        // Acquire locks in proper order to avoid deadlocks
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        old_buf = _hash_getbuf(rel, old_blkno, HASH_READ, LH_BUCKET_PAGE);

        so->hashso_split_bucket_buf = old_buf;
        LockBuffer(old_buf, BUFFER_LOCK_UNLOCK);

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        opaque = HashPageGetOpaque(page);

        if (H_BUCKET_BEING_POPULATED(opaque))
            so->hashso_buc_populated = true;
        else
        {
            _hash_dropbuf(rel, so->hashso_split_bucket_buf);
            so->hashso_split_bucket_buf = InvalidBuffer;
        }
    }

    // For backward scans, move to end of bucket chain
    if (ScanDirectionIsBackward(dir))
    {
        while (BlockNumberIsValid(opaque->hasho_nextblkno) ||
               (so->hashso_buc_populated && !so->hashso_buc_split))
            _hash_readnext(scan, &buf, &page, &opaque);
    }

    so->currPos.buf = buf;

    // Load qualifying tuples from the page
    if (!_hash_readpage(scan, &buf, dir))
        return false;

    // Return first qualifying tuple
    currItem = &so->currPos.items[so->currPos.itemIndex];
    scan->xs_heaptid = currItem->heapTid;

    return true;
}
```