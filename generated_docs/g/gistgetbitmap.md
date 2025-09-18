# gistgetbitmap

## Location
src/backend/access/gist/gistget.c: 743 - 792

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