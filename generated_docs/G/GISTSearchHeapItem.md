# GISTSearchHeapItem

## Location
[src/include/access/gist_private.h:118-127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gist_private.h#L118-L127)

## Overview
GISTSearchHeapItem represents an individual heap tuple that needs to be visited during a GiST index search operation, containing pointer information and metadata for processing.

## Definition

```c
typedef struct GISTSearchHeapItem
{
	ItemPointerData heapPtr;
	bool		recheck;		/* T if quals must be rechecked */
	bool		recheckDistances;	/* T if distances must be rechecked */
	HeapTuple	recontup;		/* data reconstructed from the index, used in
								 * index-only scans */
	OffsetNumber offnum;		/* track offset in page to mark tuple as
								 * LP_DEAD */
} GISTSearchHeapItem;
```
## Detailed Description
GISTSearchHeapItem is a key component of the GiST search infrastructure that represents heap tuples awaiting visitation during index scans. This structure is part of the pairing heap-based queue system used to manage unvisited items during both ordered and non-ordered searches. During ordered searches, these items are processed according to distance ordering, while in non-ordered searches they are prioritized to ensure depth-first traversal order (heap tuples before index pages).

The structure contains essential information for tuple processing, including the physical pointer to the heap tuple, flags indicating whether recheck operations are needed, and support for index-only scans through reconstructed tuple data. The offset tracking capability enables marking tuples as dead when necessary.

## Parameters / Member Variables
- : ItemPointerData containing the physical pointer (block number and offset) to the actual heap tuple
- : Boolean flag indicating whether the search qualifiers must be rechecked against the actual heap tuple
- : Boolean flag indicating whether distance calculations must be rechecked for ordered searches
- : HeapTuple containing data reconstructed from the index, used specifically for index-only scan operations
- : OffsetNumber tracking the offset within a page, used to mark the tuple as LP_DEAD when necessary

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerData](../I/ItemPointerData.md)
  - HeapTuple
  - OffsetNumber
- Called from (representative examples):
  - [GISTSearchItem](GISTSearchItem.md)
  - [GISTScanOpaqueData](GISTScanOpaqueData.md)

## Notes and Other Information
This structure is primarily used within the GiST search queue management system, where it's embedded in GISTSearchItem structures. The recheck flags are crucial for maintaining correctness in lossy index operations where the index might provide false positives. The recontup field enables efficient index-only scans by avoiding heap access when all required data can be reconstructed from the index. The structure plays a vital role in the pairing heap-based priority queue that ensures optimal search order during GiST traversals.