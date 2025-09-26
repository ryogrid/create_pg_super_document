# tbm_lossify

## Location
[src/backend/nodes/tidbitmap.c:1355-1423](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L1355-L1423)

## Overview
Reduces memory usage of a TID bitmap by converting some exact page entries to lossy format when memory limits are exceeded.

## Definition
```c
static void tbm_lossify(TIDBitmap *tbm)
```

## Detailed Description
This function is PostgreSQL's primary memory management mechanism for TID bitmaps. When the number of entries exceeds the configured memory limit (maxentries), this function reduces memory usage by converting individual page entries into compressed chunk representations.

The algorithm works by:
1. **Iterating through the hash table**: Starting from a remembered position (lossify_start) to ensure fair distribution across the table
2. **Skipping unsuitable candidates**: Avoids pages that are already chunks or would become chunk headers (which wouldn't save memory)
3. **Converting pages to lossy**: Calls tbm_mark_page_lossy() to perform the actual conversion
4. **Target reduction**: Aims to reduce entries to maxentries/2 to avoid immediate re-triggering
5. **Adaptive limits**: If the target cannot be reached, doubles maxentries to prevent performance degradation

The current implementation is intentionally simple, using "essentially random order" rather than optimizing based on bit density. The comments acknowledge this as a potential area for future improvement.

## Parameters / Member Variables
- `tbm`: Pointer to the TIDBitmap structure to lossify

## Dependencies
- Functions called/Symbols referenced:
  - pagetable_start_iterate_at (to begin hash table iteration)
  - pagetable_iterate (to iterate through entries)
  - [tbm_mark_page_lossy](tbm_mark_page_lossy.md) (to convert individual pages)
- Types used:
  - [TIDBitmap](../T/TIDBitmap.md)
  - [PagetableEntry](../P/PagetableEntry.md)
  - pagetable_iterator
- Constants used:
  - TBM_NOT_ITERATING, TBM_HASH (status constants)
  - PAGES_PER_CHUNK (chunk size definition)
  - INT_MAX (for limit calculations)
- Called from:
  - [tbm_add_tuples](tbm_add_tuples.md)
  - [tbm_add_page](tbm_add_page.md)
  - [tbm_union_page](tbm_union_page.md)
  - Referenced in TBMSharedIterator

## Notes and Other Information
- This is a static function, only accessible within tidbitmap.c
- Requires bitmap to be in TBM_HASH state and not currently iterating
- Uses a simple but effective algorithm that could be optimized based on bit density
- Remembers iteration position (lossify_start) to ensure fair treatment of all entries
- Includes adaptive behavior to prevent performance degradation with large bitmaps
- Target is maxentries/2 rather than maxentries to reduce frequency of re-lossification
- Can dynamically increase maxentries if memory reduction targets cannot be met
- Critical for maintaining reasonable memory usage in queries with large result sets