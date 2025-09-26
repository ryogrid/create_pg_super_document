# TBMIterateResult

## Location
[src/include/nodes/tidbitmap.h:40-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/tidbitmap.h#L40-L47)

## Overview
TBMIterateResult is a structure that represents the result of iterating over a TID (Tuple ID) bitmap, containing information about tuples found on a specific database page during bitmap heap scans.

## Definition

```c
typedef struct TBMIterateResult
{
	BlockNumber blockno;		/* page number containing tuples */
	int			ntuples;		/* -1 indicates lossy result */
	bool		recheck;		/* should the tuples be rechecked? */
	/* Note: recheck is always true if ntuples < 0 */
	OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];
} TBMIterateResult;
```
## Detailed Description
TBMIterateResult serves as the return type for TID bitmap iteration functions, particularly  and . This structure encapsulates the results of scanning a TID bitmap, which is used extensively in PostgreSQL's bitmap index scans to efficiently identify which tuples on which pages need to be examined.

The structure supports both exact and lossy storage modes:
- **Exact mode**: When , the structure contains the precise offset numbers of tuples on the specified page
- **Lossy mode**: When , only the page number is known, and all tuples on that page must be examined

This design allows PostgreSQL to handle very large result sets by trading precision for memory efficiency when necessary.

## Parameters / Member Variables
- : The block (page) number in the relation that contains the tuples of interest
- : The number of tuples found on this page. A value of -1 indicates "lossy" storage, meaning individual tuple locations are not tracked
- : Boolean flag indicating whether the tuples should be rechecked against the original query conditions. Always true when ntuples < 0 (lossy mode)
- : Flexible array member containing the specific OffsetNumbers (tuple positions within the page) when operating in exact mode. Empty when in lossy mode

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro for variable-length array)
  - BlockNumber (type for page numbers)
  - OffsetNumber (type for tuple offsets within pages)

- Called from (representative examples):
  - [tbm_iterate](../t/tbm_iterate.md)
  - [tbm_shared_iterate](../t/tbm_shared_iterate.md)
  - [BitmapHeapNext](../B/BitmapHeapNext.md)
  - [heapam_scan_bitmap_next_block](../h/heapam_scan_bitmap_next_block.md)
  - [heapam_scan_bitmap_next_tuple](../h/heapam_scan_bitmap_next_tuple.md)
  - [BitmapPrefetch](../B/BitmapPrefetch.md)

## Notes and Other Information
- This structure is central to PostgreSQL's bitmap heap scan execution, enabling efficient tuple retrieval during index scans
- The flexible array member  allows the structure to accommodate varying numbers of tuples per page without wasting memory
- The lossy storage capability () is crucial for handling large result sets that would otherwise consume excessive memory
- When  is true, the executor must verify that tuples actually satisfy the query conditions, as bitmap scans may produce false positives
- Used extensively in bitmap index scans, which are particularly effective for queries with multiple WHERE conditions that can use different indexes
- The structure is defined in  and is part of PostgreSQL's tuple ID bitmap subsystem