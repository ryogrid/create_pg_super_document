# TIDBitmap

## Location
[src/backend/nodes/tidbitmap.c:149-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L149-L177)

## Overview
TIDBitmap is the main data structure representing a complete Tuple ID Bitmap in PostgreSQL, providing efficient storage and manipulation of tuple identifiers for bitmap index scans and set operations.

## Definition

```c
struct TIDBitmap
{
	NodeTag		type;			/* to make it a valid Node */
	MemoryContext mcxt;			/* memory context containing me */
	TBMStatus	status;			/* see codes above */
	struct pagetable_hash *pagetable;	/* hash table of PagetableEntry's */
	int			nentries;		/* number of entries in pagetable */
	int			maxentries;		/* limit on same to meet maxbytes */
	int			npages;			/* number of exact entries in pagetable */
	int			nchunks;		/* number of lossy entries in pagetable */
	TBMIteratingState iterating;	/* tbm_begin_iterate called? */
	uint32		lossify_start;	/* offset to start lossifying hashtable at */
	PagetableEntry entry1;		/* used when status == TBM_ONE_PAGE */
	/* these are valid when iterating is true: */
	PagetableEntry **spages;	/* sorted exact-page list, or NULL */
	PagetableEntry **schunks;	/* sorted lossy-chunk list, or NULL */
	dsa_pointer dsapagetable;	/* dsa_pointer to the element array */
	dsa_pointer dsapagetableold;	/* dsa_pointer to the old element array */
	dsa_pointer ptpages;		/* dsa_pointer to the page array */
	dsa_pointer ptchunks;		/* dsa_pointer to the chunk array */
	dsa_area   *dsa;			/* reference to per-query dsa area */
};
```
## Detailed Description
TIDBitmap serves as PostgreSQL's primary mechanism for efficiently representing and manipulating sets of tuple identifiers (TIDs). It supports multiple operational modes optimized for different scenarios:

1. **Single-page optimization**: When only one page is referenced, the bitmap uses the embedded  field to avoid hashtable overhead.

2. **Multi-page exact storage**: Uses a hashtable of PagetableEntry structures for precise tuple identification within pages.

3. **Lossy storage**: When memory constraints are reached, the bitmap can convert exact page entries to lossy chunk entries, trading precision for memory efficiency.

The structure also supports both local and shared memory contexts for parallel query execution, with DSA (Dynamic Shared Area) pointers enabling coordination across multiple worker processes.

## Parameters / Member Variables
- `type`: NodeTag identifier making this a valid PostgreSQL Node for memory management and type checking
- `mcxt`: Memory context that owns this TIDBitmap instance, controlling allocation and cleanup
- `status`: Current operational status (TBM_EMPTY, TBM_ONE_PAGE, TBM_MULTIPLE_PAGES)
- `*pagetable`: Hash table containing PagetableEntry objects for multi-page storage
- `nentries`: Total number of entries currently in the pagetable
- `maxentries`: Maximum allowed entries before triggering lossification to stay within memory limits
- `npages`: Count of exact page entries in the pagetable
- `nchunks`: Count of lossy chunk entries in the pagetable
- `iterating`: State flag indicating whether iteration has been initiated via tbm_begin_iterate
- `lossify_start`: Hashtable offset for starting the lossification process when memory pressure occurs
- `entry1`: Embedded PagetableEntry used for single-page optimization to avoid hashtable overhead
- `**spages`: Sorted array of exact PagetableEntry pointers, populated during iteration preparation
- `**schunks`: Sorted array of lossy chunk PagetableEntry pointers, populated during iteration preparation
- `dsapagetable`: DSA pointer to shared element array for parallel execution
- `dsapagetableold`: DSA pointer to previous element array during transitions
- `ptpages`: DSA pointer to shared page array for parallel workers
- `ptchunks`: DSA pointer to shared chunk array for parallel workers
- `*dsa`: Reference to per-query dynamic shared area for parallel processing coordination

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