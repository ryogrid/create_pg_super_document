# gistgettuple

## Location
[src/backend/access/gist/gistget.c:612-742](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistget.c#L612-L742)

## Overview
gistgettuple retrieves the next tuple in a GiST index scan, supporting both ordered (distance-based) and unordered scan modes while managing scan state and killed item tracking.

## Definition
```c
bool gistgettuple(IndexScanDesc scan, ScanDirection dir)
```

## Detailed Description
gistgettuple is the main tuple retrieval function for GiST index scans. It handles two distinct scan modes: ordered scans for nearest-neighbor queries (using ORDER BY with distance operators) and regular index scans that process pages sequentially. The function maintains scan state through the GISTScanOpaque structure, manages memory contexts for page data, and implements tuple killing optimization to mark dead index entries.

For first-time calls, it initializes the scan by processing the root page. For ordered scans (when numberOfOrderBys > 0), it delegates to getNextNearest() for strict distance ordering. For regular scans, it returns tuples page-by-page from the pageData buffer, processing new pages as needed through the search queue maintained by getNextGISTSearchItem().

The function also implements the "killed items" optimization, tracking index tuples that correspond to deleted heap tuples so they can be marked as dead in a batch operation via gistkillitems().

## Parameters
- `scan`: IndexScanDesc containing the scan descriptor with scan keys, relation info, and opaque GiST scan state
- `dir`: ScanDirection specifying scan direction (only ForwardScanDirection is supported)

## Dependencies
- Functions called/Symbols referenced:
  - [getNextNearest](getNextNearest.md) (for ordered scans)
  - [gistScanPage](gistScanPage.md) (for processing index pages)  
  - [getNextGISTSearchItem](getNextGISTSearchItem.md) (for retrieving next search item)
  - [gistkillitems](gistkillitems.md) (for marking dead tuples)
  - pgstat_count_index_scan (for statistics)
  - [MemoryContextReset](../M/MemoryContextReset.md) (for memory management)
- Called from (representative examples):
  - [gisthandler](gisthandler.md) (index AM handler setup)

## Notes and Other Information
- Only supports forward scan direction; throws error for backward scans
- Uses different strategies for ordered vs unordered scans
- Implements killed items optimization for better performance with deleted heap tuples
- Manages page data buffering to reduce I/O operations
- Part of the PostgreSQL GiST (Generalized Search Tree) access method
- Returns false when no more tuples are available, true when a tuple is found
- Sets scan->xs_heaptid, scan->xs_recheck, and scan->xs_hitup for returned tuples

## Simplified Source

```c
bool
gistgettuple(IndexScanDesc scan, ScanDirection dir)
{
    GISTScanOpaque so = (GISTScanOpaque) scan->opaque;

    // Only support forward scanning
    if (dir != ForwardScanDirection)
        elog(ERROR, "GiST only supports forward scan direction");

    if (!so->qual_ok)
        return false;

    // First call - initialize scan from root page
    if (so->firstCall)
    {
        GISTSearchItem fakeItem;

        pgstat_count_index_scan(scan->indexRelation);
        so->firstCall = false;
        so->curPageData = so->nPageData = 0;
        scan->xs_hitup = NULL;

        // Start scan from root page
        fakeItem.blkno = GIST_ROOT_BLKNO;
        memset(&fakeItem.data.parentlsn, 0, sizeof(GistNSN));
        gistScanPage(scan, &fakeItem, NULL, NULL, NULL);
    }

    // Handle ordered scans (nearest neighbor queries)
    if (scan->numberOfOrderBys > 0)
    {
        return getNextNearest(scan);  // Strict distance ordering
    }
    else
    {
        // Regular scan - process pages sequentially
        for (;;)
        {
            // Return tuples from current page buffer
            if (so->curPageData < so->nPageData)
            {
                // Track killed items for optimization
                if (scan->kill_prior_tuple && so->curPageData > 0)
                {
                    // Add previous tuple to killed items list
                    if (so->killedItems == NULL)
                        so->killedItems = (OffsetNumber *) palloc(MaxIndexTuplesPerPage * sizeof(OffsetNumber));
                    if (so->numKilled < MaxIndexTuplesPerPage)
                        so->killedItems[so->numKilled++] = so->pageData[so->curPageData - 1].offnum;
                }

                // Return next tuple from page buffer
                scan->xs_heaptid = so->pageData[so->curPageData].heapPtr;
                scan->xs_recheck = so->pageData[so->curPageData].recheck;

                if (scan->xs_want_itup)  // Index-only scan
                    scan->xs_hitup = so->pageData[so->curPageData].recontup;

                so->curPageData++;
                return true;
            }

            // Current page exhausted - get next page
            do
            {
                GISTSearchItem *item;

                // Apply killed items optimization
                if ((so->curBlkno != InvalidBlockNumber) && (so->numKilled > 0))
                    gistkillitems(scan);

                // Get next search item from queue
                item = getNextGISTSearchItem(so);
                if (!item)
                    return false;  // Scan complete

                CHECK_FOR_INTERRUPTS();
                so->curBlkno = item->blkno;

                // Scan the page to populate pageData buffer
                gistScanPage(scan, item, item->distances, NULL, NULL);
                pfree(item);

            } while (so->nPageData == 0);  // Continue until we get tuples
        }
    }
}
```