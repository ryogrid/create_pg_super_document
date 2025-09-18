# gistScanPage

## Location
[src/backend/access/gist/gistget.c:328-537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistget.c#L328-L537)

## Overview
gistScanPage scans all items on a GiST index page and processes them according to the scan type, handling concurrent splits and managing output to search queues or result arrays.

## Definition
static void gistScanPage(IndexScanDesc scan, GISTSearchItem *pageItem, IndexOrderByDistance *myDistances, TIDBitmap *tbm, int64 *ntids)

## Detailed Description
This function is responsible for scanning all tuples on a GiST index page and routing them to appropriate outputs based on scan type:

1. **Page Split Handling**: Detects concurrent page splits using parent LSN vs. NSN comparison and adds right sibling pages to the search queue when splits are detected.

2. **Tuple Processing**: For each valid tuple on the page:
   - Calls gistindex_keytest to evaluate if the tuple matches scan conditions
   - Routes matching tuples based on scan type:
     - Bitmap scans: Adds TIDs directly to TIDBitmap
     - Non-ordered scans: Stores results in pageData array
     - Ordered scans: Creates GISTSearchItems and adds to priority queue

3. **Index-Only Scan Support**: When xs_want_itup is set, reconstructs index tuples using gistFetchTuple for index-only scans.

4. **Concurrency Safety**: Saves page LSN for safe application of LP_DEAD hints later and handles deleted pages appropriately.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing scan state and configuration
- `pageItem`: GISTSearchItem identifying the index page to scan
- `myDistances`: Distance array associated with pageItem (NULL at root)
- `tbm`: Output bitmap for amgetbitmap scans (NULL for other scan types)
- `ntids`: Output tuple counter for bitmap scans (NULL for other scan types)

## Dependencies
- Functions called/Symbols referenced:
  - [ReadBuffer](../R/ReadBuffer.md)
  - [PredicateLockPage](../P/PredicateLockPage.md)
  - [gistcheckpage](gistcheckpage.md)
  - GistPageGetOpaque
  - GistFollowRight
  - GistPageGetNSN
  - GistPageIsDeleted
  - [BufferGetLSNAtomic](../B/BufferGetLSNAtomic.md)
  - [gistindex_keytest](gistindex_keytest.md)
  - [tbm_add_tuples](../t/tbm_add_tuples.md)
  - [gistFetchTuple](gistFetchTuple.md)
  - pairingheap_add
- Called from:
  - [getNextNearest](getNextNearest.md)
  - [gistgettuple](gistgettuple.md)
  - [gistgetbitmap](gistgetbitmap.md)

## Notes and Other Information
- This is a static function only accessible within gistget.c
- Handles three different scan types: bitmap, non-ordered, and ordered scans
- Uses memory contexts (tempCxt, pageDataCxt, queueCxt) for proper memory management
- Page split detection is crucial for correctness in concurrent environments
- The function must handle both leaf pages (containing heap TIDs) and internal pages (containing child page references)
- LSN tracking enables safe optimization via LP_DEAD hints