# GISTScanOpaqueData

## Location
[src/include/access/gist_private.h:154-179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gist_private.h#L154-L179)

## Overview
GISTScanOpaqueData maintains the complete private state for a GiST index scan operation, including the search queue, workspace areas, and buffers for efficient tuple retrieval.

## Definition

```c
typedef struct GISTScanOpaqueData
{
	GISTSTATE  *giststate;		/* index information, see above */
	Oid		   *orderByTypes;	/* datatypes of ORDER BY expressions */

	pairingheap *queue;			/* queue of unvisited items */
	MemoryContext queueCxt;		/* context holding the queue */
	bool		qual_ok;		/* false if qual can never be satisfied */
	bool		firstCall;		/* true until first gistgettuple call */

	/* pre-allocated workspace arrays */
	IndexOrderByDistance *distances;	/* output area for gistindex_keytest */

	/* info about killed items if any (killedItems is NULL if never used) */
	OffsetNumber *killedItems;	/* offset numbers of killed items */
	int			numKilled;		/* number of currently stored items */
	BlockNumber curBlkno;		/* current number of block */
	GistNSN		curPageLSN;		/* pos in the WAL stream when page was read */

	/* In a non-ordered search, returnable heap items are stored here: */
	GISTSearchHeapItem pageData[BLCKSZ / sizeof(IndexTupleData)];
	OffsetNumber nPageData;		/* number of valid items in array */
	OffsetNumber curPageData;	/* next item to return */
	MemoryContext pageDataCxt;	/* context holding the fetched tuples, for
								 * index-only scans */
} GISTScanOpaqueData;
```
## Detailed Description
GISTScanOpaqueData serves as the comprehensive state holder for GiST index scan operations, encapsulating all necessary information for both ordered and non-ordered searches. The structure manages the core search infrastructure including the pairing heap-based priority queue for unvisited items, workspace areas for distance calculations, and specialized buffers for efficient tuple retrieval during non-ordered scans.

The design accommodates both distance-ordered searches (using ORDER BY clauses) and regular searches, with different optimization strategies for each. For non-ordered searches, the pageData array acts as a local buffer to collect all returnable items from a page before processing, improving efficiency by reducing queue operations. The structure also includes facilities for tracking killed items and managing memory contexts for different aspects of the scan operation.

## Parameters / Member Variables
- `*giststate`: Pointer to GISTSTATE containing all index-specific information and cached support functions
- `*orderByTypes`: Array of Oid values representing the datatypes of ORDER BY expressions in ordered searches
- `*queue`: Pairing heap managing the queue of unvisited GISTSearchItem entries
- `queueCxt`: Memory context that holds the search queue and related data structures
- `qual_ok`: Boolean flag indicating whether the search qualifiers can ever be satisfied (false means early termination)
- `firstCall`: Boolean flag tracking whether this is the first call to gistgettuple for initialization purposes
- `*distances`: Pre-allocated workspace array for storing distance calculation results from gistindex_keytest
- `*killedItems`: Array of offset numbers for items that have been marked as killed/dead
- `numKilled`: Current count of items stored in the killedItems array
- `curBlkno`: Block number of the page currently being processed
- `curPageLSN`: LSN (Log Sequence Number) position in the WAL stream when the current page was read
- `pageData[BLCKSZ  sizeof(IndexTupleData)]`: Fixed-size array storing returnable heap items for non-ordered searches
- `nPageData`: Number of valid entries currently stored in the pageData array
- `curPageData`: Index of the next item to return from the pageData array
- `pageDataCxt`: Memory context holding fetched tuples specifically for index-only scan operations
## Dependencies
- Functions called/Symbols referenced:
  - [GISTSTATE](GISTSTATE.md)
  - [pairingheap](../p/pairingheap.md)
  - [IndexOrderByDistance](../I/IndexOrderByDistance.md)
  - GistNSN
  - [GISTSearchHeapItem](GISTSearchHeapItem.md)
  - [IndexTupleData](../I/IndexTupleData.md)
  - [MemoryContext](../M/MemoryContext.md)
  - Oid
  - OffsetNumber
  - BlockNumber
- Called from (representative examples):
  - [gistbeginscan](../g/gistbeginscan.md)
  - GISTScanOpaque (typedef)

## Notes and Other Information
This structure is typically allocated and initialized during gistbeginscan() and persists throughout the entire scan operation. The pageData array optimization is particularly important for non-ordered scans, as it allows batching of returnable items from each page rather than processing them individually through the queue system. The dual memory context design (queueCxt and pageDataCxt) enables fine-grained memory management for different aspects of the scan. The killed items tracking mechanism supports PostgreSQL's tuple visibility and cleanup operations during concurrent access scenarios.