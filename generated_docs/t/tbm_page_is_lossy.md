# tbm_page_is_lossy

## Location
[src/backend/nodes/tidbitmap.c:1249-1282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L1249-L1282)

## Overview
Determines whether a specific page is marked as lossily stored in a TID bitmap's lossy chunks.

## Definition
```c
static bool tbm_page_is_lossy(const TIDBitmap *tbm, BlockNumber pageno)
```

## Detailed Description
This function checks if a given page number is stored in lossy format within the TID bitmap. Lossy storage occurs when the bitmap exceeds memory limits and converts exact page entries into compressed chunk representations where individual tuple positions are lost, but page-level information is retained.

The function operates by:
1. Early exit if no lossy chunks exist (tbm->nchunks == 0)
2. Calculating which chunk the page belongs to using PAGES_PER_CHUNK
3. Looking up the chunk entry in the page table
4. If a chunk entry exists, checking the specific bit for the page within that chunk's bitmap

This is an optimization mechanism - when memory pressure forces the system to use lossy representation, this function quickly determines if a page is affected without needing to reconstruct exact tuple information.

## Parameters / Member Variables
- `tbm`: Pointer to the TIDBitmap structure (const, read-only access)
- `pageno`: Block number of the page to check for lossy storage

## Dependencies
- Functions called/Symbols referenced:
  - pagetable_lookup (to find chunk entries in hash table)
- Types used:
  - TIDBitmap
  - PagetableEntry
  - BlockNumber
  - bitmapword
- Constants/Macros used:
  - TBM_HASH (bitmap status constant)
  - PAGES_PER_CHUNK (chunk size definition)
  - WORDNUM, BITNUM (bit manipulation macros)
- Called from:
  - tbm_add_tuples
  - tbm_union_page  
  - tbm_intersect_page (multiple locations)
  - Referenced in TBMSharedIterator

## Notes and Other Information
- This is a static function, only accessible within tidbitmap.c
- Uses const qualifier indicating read-only operation on the bitmap
- Early optimization check for tbm->nchunks == 0 avoids unnecessary computation
- Only operates when bitmap is in TBM_HASH state (asserted)
- Essential for bitmap operations to understand when exact tuple information is unavailable
- Part of the memory management strategy for large TID bitmaps