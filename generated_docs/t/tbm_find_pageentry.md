# tbm_find_pageentry

## Location
[src/backend/nodes/tidbitmap.c:1169-1201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L1169-L1201)

## Overview
Finds and returns a PagetableEntry for a specific page number, ensuring the entry is non-lossy (exact tuple information available).

## Definition

```c
static const PagetableEntry *
tbm_find_pageentry(const TIDBitmap *tbm, BlockNumber pageno)
```
## Detailed Description
The  function performs a lookup operation to find a specific PagetableEntry within a TIDBitmap structure. It specifically searches for non-lossy entries, returning NULL if the requested page is either not found or exists only as a lossy chunk header.

The function handles different bitmap states: when the bitmap contains only a single page (TBM_ONE_PAGE status), it directly checks the embedded entry1. For multi-page bitmaps, it uses the pagetable hash table for efficient lookup. The function ensures that only exact page entries are returned, filtering out chunk headers that represent lossy compressed data.

## Parameters / Member Variables
- : const TIDBitmap pointer to the bitmap being searched
- : BlockNumber specifying the page number to find

## Dependencies
- Functions called/Symbols referenced:
  - pagetable_lookup
  - [TIDBitmap](../T/TIDBitmap.md)
  - [PagetableEntry](../P/PagetableEntry.md)
  - TBM_ONE_PAGE
  - BlockNumber
- Called from (representative examples):
  - tbm_intersect_page (src/backend/nodes/tidbitmap.c:616, 648)
  - [TBMSharedIterator](../T/TBMSharedIterator.md) (src/backend/nodes/tidbitmap.c:232)

## Notes and Other Information
This is a static function internal to tidbitmap.c, primarily used for bitmap intersection operations. The function explicitly rejects lossy chunk headers (where ischunk is true) to ensure callers receive only exact tuple position information. Returns NULL for non-existent pages or when only lossy information is available for the requested page.