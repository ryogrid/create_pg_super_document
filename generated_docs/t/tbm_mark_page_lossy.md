# tbm_mark_page_lossy

## Location
src/backend/nodes/tidbitmap.c: 1283 - 1354

## Overview
Marks a specific page number as lossily stored in a TID bitmap by creating or updating chunk entries in the hash table.

## Definition
```c
static void tbm_mark_page_lossy(TIDBitmap *tbm, BlockNumber pageno)
```

## Detailed Description
This function converts a page to lossy storage format, which is a key part of PostgreSQL's memory management strategy for TID bitmaps. When exact tuple information cannot be maintained due to memory constraints, pages are marked as lossy - meaning only the page-level information is retained.

The function performs several critical operations:
1. **Force hash table mode**: Ensures the bitmap is in TBM_HASH state by calling tbm_create_pagetable() if needed
2. **Remove existing exact entries**: Deletes any existing non-lossy entry for the target page (unless it's a chunk header)
3. **Create/update chunk entry**: Finds or creates a chunk entry for the page range containing the target page
4. **Handle chunk conversion**: If the chunk header was previously non-lossy, converts it to lossy format
5. **Set the bit**: Marks the specific page within the chunk's bitmap

The chunk-based approach allows multiple pages to be represented in a compressed format where PAGES_PER_CHUNK pages are represented by a single bitmap entry.

## Parameters / Member Variables
- `tbm`: Pointer to the TIDBitmap structure to modify
- `pageno`: Block number of the page to mark as lossy

## Dependencies
- Functions called/Symbols referenced:
  - tbm_create_pagetable (to ensure hash table mode)
  - pagetable_delete (to remove existing exact entries)
  - pagetable_insert (to create/find chunk entries)
  - MemSet (to initialize page entries)
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
  - tbm_add_page
  - tbm_union_page
  - tbm_lossify
  - Referenced in TBMSharedIterator

## Notes and Other Information
- This is a static function, only accessible within tidbitmap.c
- Forces the bitmap into hashtable mode regardless of current state
- May cause memory usage to exceed limits - caller should invoke tbm_lossify() afterward
- Handles complex chunk header conversion logic when existing pages become chunk headers
- Updates multiple counters: nentries, npages, nchunks
- The function carefully manages the transition from exact to lossy representation
- Critical for the bitmap's ability to handle large result sets within memory constraints