# TIDBitmap

## Location
[src/backend/nodes/tidbitmap.c:149-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L149-L177)

## Overview
TIDBitmap is the main data structure representing a complete Tuple ID Bitmap in PostgreSQL, providing efficient storage and manipulation of tuple identifiers for bitmap index scans and set operations.

## Definition


## Detailed Description
TIDBitmap serves as PostgreSQL's primary mechanism for efficiently representing and manipulating sets of tuple identifiers (TIDs). It supports multiple operational modes optimized for different scenarios:

1. **Single-page optimization**: When only one page is referenced, the bitmap uses the embedded  field to avoid hashtable overhead.

2. **Multi-page exact storage**: Uses a hashtable of PagetableEntry structures for precise tuple identification within pages.

3. **Lossy storage**: When memory constraints are reached, the bitmap can convert exact page entries to lossy chunk entries, trading precision for memory efficiency.

The structure also supports both local and shared memory contexts for parallel query execution, with DSA (Dynamic Shared Area) pointers enabling coordination across multiple worker processes.

## Parameters / Member Variables
- : NodeTag identifier making this a valid PostgreSQL Node for memory management and type checking
- : Memory context that owns this TIDBitmap instance, controlling allocation and cleanup
- : Current operational status (TBM_EMPTY, TBM_ONE_PAGE, TBM_MULTIPLE_PAGES)
- : Hash table containing PagetableEntry objects for multi-page storage
- : Total number of entries currently in the pagetable
- : Maximum allowed entries before triggering lossification to stay within memory limits
- : Count of exact page entries in the pagetable
- : Count of lossy chunk entries in the pagetable
- : State flag indicating whether iteration has been initiated via tbm_begin_iterate
- : Hashtable offset for starting the lossification process when memory pressure occurs
- : Embedded PagetableEntry used for single-page optimization to avoid hashtable overhead
- : Sorted array of exact PagetableEntry pointers, populated during iteration preparation
- : Sorted array of lossy chunk PagetableEntry pointers, populated during iteration preparation
- : DSA pointer to shared element array for parallel execution
- : DSA pointer to previous element array during transitions
- : DSA pointer to shared page array for parallel workers
- : DSA pointer to shared chunk array for parallel workers  
- : Reference to per-query dynamic shared area for parallel processing coordination

## Dependencies
- Functions called/Symbols referenced:
  - TBMStatus
  - TBMIteratingState
  - [PagetableEntry](../P/PagetableEntry.md)
  - dsa_pointer
  - dsa_area
- Called from (representative examples):
  - [index_getbitmap](../i/index_getbitmap.md) (from various index access methods)
  - [MultiExecBitmapAnd](../M/MultiExecBitmapAnd.md)
  - [MultiExecBitmapOr](../M/MultiExecBitmapOr.md)
  - [BitmapHeapNext](../B/BitmapHeapNext.md)
  - [tbm_create](../t/tbm_create.md)
  - tbm_union
  - tbm_intersect

## Notes and Other Information
- [TIDBitmap](TIDBitmap.md) is central to PostgreSQL's bitmap index scan optimization, enabling efficient processing of queries involving multiple indexes
- The lossification mechanism allows graceful degradation under memory pressure, maintaining query correctness while reducing memory consumption
- Supports both AND and OR operations between multiple bitmaps, essential for complex query optimization
- The dual-mode operation (exact vs lossy) balances precision with memory efficiency, critical for large result sets
- DSA integration enables efficient parallel bitmap operations across multiple worker processes
- The sorted arrays (spages, schunks) enable efficient iteration over bitmap contents in block number order