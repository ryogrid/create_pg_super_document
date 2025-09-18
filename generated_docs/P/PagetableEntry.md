# PagetableEntry

## Location
[src/backend/nodes/tidbitmap.c:100-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L100-L107)

## Overview
PagetableEntry is a data structure that represents hashtable entries in PostgreSQL's Tuple ID Bitmap (TIDBitmap) system, supporting both exact page storage and lossy chunk storage for memory-efficient bitmap operations.

## Definition


## Detailed Description
PagetableEntry serves as the core data structure for PostgreSQL's TIDBitmap system, which is used for efficient storage and manipulation of tuple identifiers. The structure supports two distinct storage modes:

1. **Exact page mode**:  represents a specific page number, and each bit k in the bitmap represents tuple offset k+1 within that page.

2. **Lossy chunk mode**:  represents the first page in a chunk (must be a multiple of PAGES_PER_CHUNK), and each bit k represents page blockno+k. This mode sacrifices precision for memory efficiency when dealing with large result sets.

The same hashtable entry cannot simultaneously serve both exact and lossy purposes for overlapping page ranges, ensuring consistency in the bitmap representation.

## Parameters / Member Variables
- : Page number that serves as the hashtable key; for exact storage, this is the specific page number; for lossy storage, this is the first page number of the chunk
- : Hash entry status used by the hashtable implementation for managing entry lifecycle
- : Boolean flag indicating storage mode - true for lossy chunk storage, false for exact page storage
- : Boolean flag used only for exact pages, indicating whether tuples are candidate matches that require full index qualification condition checking
- : Bitmap array sized to accommodate either page-level or chunk-level storage requirements, using the maximum of WORDS_PER_PAGE and WORDS_PER_CHUNK

## Dependencies
- Functions called/Symbols referenced:
  - WORDS_PER_PAGE
  - WORDS_PER_CHUNK
  - bitmapword
- Called from (representative examples):
  - [tbm_create_pagetable](../t/tbm_create_pagetable.md)
  - [tbm_add_tuples](../t/tbm_add_tuples.md)
  - tbm_union_page
  - tbm_intersect_page
  - [tbm_iterate](../t/tbm_iterate.md)
  - [tbm_find_pageentry](../t/tbm_find_pageentry.md)
  - tbm_get_pageentry

## Notes and Other Information
- The structure is designed to optimize memory usage while maintaining query performance through its dual-mode operation
- The recheck mechanism ensures correctness in index scans where bitmap entries represent candidate matches rather than definitive results
- [PagetableEntry](PagetableEntry.md) is fundamental to PostgreSQL's bitmap index scan optimization, particularly important for queries involving multiple indexes or large result sets
- The lossy chunk mechanism allows the system to gracefully handle memory pressure by trading precision for reduced memory consumption