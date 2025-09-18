# gistgettuple

## Location
src/backend/access/gist/gistget.c: 612 - 742

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