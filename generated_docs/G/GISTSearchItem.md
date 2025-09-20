# GISTSearchItem

## Location
[src/include/access/gist_private.h:130-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gist_private.h#L130-L143)

## Overview
GISTSearchItem represents a generic unvisited item in the GiST search queue, which can be either an index page or a heap tuple, managed within a pairing heap for efficient ordered traversal.

## Definition

```c
typedef struct GISTSearchItem
{
	pairingheap_node phNode;
	BlockNumber blkno;			/* index page number, or InvalidBlockNumber */
	union
	{
		GistNSN		parentlsn;	/* parent page's LSN, if index page */
		/* we must store parentlsn to detect whether a split occurred */
		GISTSearchHeapItem heap;	/* heap info, if heap tuple */
	}			data;

	/* numberOfOrderBys entries */
	IndexOrderByDistance distances[FLEXIBLE_ARRAY_MEMBER];
} GISTSearchItem;
```
## Detailed Description
GISTSearchItem serves as the fundamental unit in the GiST search queue system, providing a unified representation for both index pages and heap tuples that await processing during index scans. The structure is designed to work within a pairing heap data structure (via the phNode member) to enable efficient priority-based retrieval during ordered searches and proper depth-first ordering during non-ordered searches.

The key design feature is the union that allows the same structure to represent two different types of search targets: index pages (identified by blkno and tracked via parentlsn for split detection) and heap tuples (containing full heap item information). The flexible array member for distances enables support for multiple ORDER BY clauses in distance-ordered searches, making this structure central to PostgreSQL's nearest-neighbor search capabilities.

## Parameters / Member Variables
- `phNode`: Pairing heap node structure that enables this item to be stored and managed in the priority queue
- `blkno`: Block number of the index page being referenced, or InvalidBlockNumber if this represents a heap tuple
- `parentlsn`: When representing an index page, stores the parent page's LSN to detect concurrent splits during traversal
- `heap`: When representing a heap tuple, contains the complete GISTSearchHeapItem with heap pointer and metadata
- `distances`: Variable-length array containing distance values for each ORDER BY clause in ordered searches

## Dependencies
- Functions called/Symbols referenced:
  - [pairingheap_node](../p/pairingheap_node.md)
  - BlockNumber
  - GistNSN
  - [GISTSearchHeapItem](GISTSearchHeapItem.md)
  - [IndexOrderByDistance](../I/IndexOrderByDistance.md)
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [gistScanPage](../g/gistScanPage.md)
  - [getNextGISTSearchItem](../g/getNextGISTSearchItem.md)
  - [getNextNearest](../g/getNextNearest.md)
  - [gistgettuple](../g/gistgettuple.md)
  - [pairingheap_GISTSearchItem_cmp](../p/pairingheap_GISTSearchItem_cmp.md)

## Notes and Other Information
The structure's dual nature (index page vs heap tuple) is determined by examining the blkno field - InvalidBlockNumber indicates a heap tuple, while a valid block number indicates an index page. The parentlsn tracking is crucial for detecting concurrent page splits during long-running scans, ensuring consistency even when the index structure changes during traversal. The flexible array member for distances makes this structure size-variable depending on the number of ORDER BY clauses, requiring careful memory allocation using SizeOfGISTSearchItem. This design enables efficient implementation of PostgreSQL's k-nearest neighbor search functionality in GiST indexes.