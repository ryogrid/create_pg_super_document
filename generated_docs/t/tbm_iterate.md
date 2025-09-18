# tbm_iterate

## Location
[src/backend/nodes/tidbitmap.c:971-1051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L971-L1051)

## Overview
Scans through the next page of a TIDBitmap during iteration, returning pages in numerical order with support for both exact and lossy tuple identification.

## Definition


## Detailed Description
The  function is the core iteration mechanism for TIDBitmap structures in PostgreSQL. It processes bitmap data to return the next page that contains tuples matching query conditions. The function handles both lossy chunk pages (where exact tuple positions are not remembered) and exact pages with specific tuple offsets.

The function maintains iteration state through the TBMIterator, ensuring pages are delivered in numerical order by comparing chunk block numbers with individual page block numbers. When lossy chunks are encountered, the function returns a result with  to indicate the caller must examine all tuples on the page. For exact pages, it extracts specific tuple offsets using .

## Parameters / Member Variables
- : TBMIterator pointer containing iteration state including current positions in chunks and pages arrays

## Dependencies
- Functions called/Symbols referenced:
  - [tbm_advance_schunkbit](tbm_advance_schunkbit.md)
  - [tbm_extract_page_tuple](tbm_extract_page_tuple.md)
  - [TBMIterator](../T/TBMIterator.md)
  - [TIDBitmap](../T/TIDBitmap.md)
  - TBMIterateResult
  - [PagetableEntry](../P/PagetableEntry.md)
  - TBM_ITERATING_PRIVATE
  - TBM_ONE_PAGE
  - PAGES_PER_CHUNK
- Called from (representative examples):
  - [entryGetItem](../e/entryGetItem.md) (src/backend/access/gin/ginget.c:837)
  - [BitmapHeapNext](../B/BitmapHeapNext.md) (src/backend/executor/nodeBitmapHeapscan.c:239)
  - [BitmapAdjustPrefetchIterator](../B/BitmapAdjustPrefetchIterator.md) (src/backend/executor/nodeBitmapHeapscan.c:376)
  - [BitmapPrefetch](../B/BitmapPrefetch.md) (src/backend/executor/nodeBitmapHeapscan.c:475)

## Notes and Other Information
The function ensures numerical page order by carefully comparing chunk and individual page block numbers. When  is true, the condition must be rechecked even for exact tuples. The function returns NULL when no more pages remain in the bitmap. The iteration state is private to a single process (contrast with  for multi-process scenarios).