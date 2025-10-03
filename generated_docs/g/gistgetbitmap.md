# gistgetbitmap

## Location
[src/backend/access/gist/gistget.c:743-792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistget.c#L743-L792)

## Overview
gistgetbitmap performs a bitmap index scan on a GiST index, collecting all matching heap tuple locations into a TID bitmap for efficient batch retrieval.

## Definition
```c
int64 gistgetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
```

## Detailed Description
gistgetbitmap implements bitmap index scanning for GiST indexes, which is used in bitmap heap scan operations. Unlike gistgettuple which returns tuples one at a time, this function traverses the entire qualifying portion of the index and collects all matching heap tuple identifiers (TIDs) into a bitmap structure.

The function begins by processing the root page and then systematically traverses all qualifying index pages using the search queue mechanism. As it encounters leaf pages with matching entries, the heap TIDs are added directly to the provided TID bitmap (tbm) through gistScanPage. This approach allows the optimizer to combine multiple index scans and perform efficient batch processing of heap tuple retrieval.

The function is simpler than gistgettuple because it doesn't need to maintain complex scan state for incremental tuple retrieval - it processes the entire qualifying result set in one pass.

## Parameters
- `scan`: IndexScanDesc containing the scan descriptor with scan keys, relation info, and GiST scan state
- `tbm`: TIDBitmap structure where matching heap tuple identifiers will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [gistScanPage](gistScanPage.md) (for processing index pages and collecting TIDs)
  - [getNextGISTSearchItem](getNextGISTSearchItem.md) (for retrieving next search item from queue)
  - pgstat_count_index_scan (for statistics tracking)
  - [MemoryContextReset](../M/MemoryContextReset.md) (for memory management)
- Called from (representative examples):
  - [gisthandler](gisthandler.md) (index AM handler setup)

## Notes and Other Information
- Returns the total number of heap TIDs collected in the bitmap
- More efficient than tuple-at-a-time scanning for large result sets
- Used in bitmap heap scan operations where multiple indexes can be combined
- Does not support ordered scans (ORDER BY clauses) - only qualification-based filtering
- Simpler than gistgettuple as it doesn't maintain incremental scan state
- Part of PostgreSQL's bitmap scanning optimization for better I/O patterns
- Returns 0 immediately if scan qualifications are not satisfiable (!qual_ok)
- Memory management is simpler as no persistent page data buffering is needed

## Simplified Source

```c
int64
gistgetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
    GISTScanOpaque so = (GISTScanOpaque) scan->opaque;
    int64 ntids = 0;
    GISTSearchItem fakeItem;

    if (!so->qual_ok)
        return 0;

    pgstat_count_index_scan(scan->indexRelation);

    // Initialize scan state
    so->curPageData = so->nPageData = 0;
    scan->xs_hitup = NULL;
    if (so->pageDataCxt)
        MemoryContextReset(so->pageDataCxt);

    // Start scan from root page
    fakeItem.blkno = GIST_ROOT_BLKNO;
    memset(&fakeItem.data.parentlsn, 0, sizeof(GistNSN));
    gistScanPage(scan, &fakeItem, NULL, tbm, &ntids);

    // Process all pages in search queue
    for (;;)
    {
        GISTSearchItem *item = getNextGISTSearchItem(so);

        if (!item)
            break;  // No more items

        CHECK_FOR_INTERRUPTS();

        // Scan page and add matching TIDs to bitmap
        gistScanPage(scan, item, item->distances, tbm, &ntids);
        pfree(item);
    }

    return ntids;  // Total number of TIDs found
}
```