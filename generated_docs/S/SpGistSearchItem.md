# SpGistSearchItem

## Location
[src/include/access/spgist_private.h:165-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgist_private.h#L165-L181)

## Overview
SpGistSearchItem represents a work item for SP-GiST index scans, containing information about index entries that need to be examined during search operations.

## Definition

```c
typedef struct SpGistSearchItem
{
	pairingheap_node phNode;	/* pairing heap node */
	Datum		value;			/* value reconstructed from parent, or
								 * leafValue if isLeaf */
	SpGistLeafTuple leafTuple;	/* whole leaf tuple, if needed */
	void	   *traversalValue; /* opclass-specific traverse value */
	int			level;			/* level of items on this page */
	ItemPointerData heapPtr;	/* heap info, if heap tuple */
	bool		isNull;			/* SearchItem is NULL item */
	bool		isLeaf;			/* SearchItem is heap item */
	bool		recheck;		/* qual recheck is needed */
	bool		recheckDistances;	/* distance recheck is needed */

	/* array with numberOfOrderBys entries */
	double		distances[FLEXIBLE_ARRAY_MEMBER];
} SpGistSearchItem;
```
## Detailed Description
SpGistSearchItem is the fundamental work unit used during SP-GiST index scans to track items that require further examination. It serves as an entry in a priority queue (implemented as a pairing heap) that manages the search process efficiently. Each item represents either an inner node that needs to be traversed or a leaf tuple that needs to be tested against search conditions.

The structure supports sophisticated search operations including distance-based ordering for nearest-neighbor queries. It maintains both the reconstructed value from traversing the tree and optional operator class-specific traversal information. This design enables SP-GiST to efficiently handle complex spatial and hierarchical search patterns while supporting features like index-only scans and distance calculations.

## Parameters / Member Variables
- `phNode`: Pairing heap node for priority queue management during search
- `value`: The data value reconstructed from traversing parent nodes, or actual leaf value for leaf items
- `leafTuple`: Complete leaf tuple data when needed for the search operation
- `*traversalValue`: Operator class-specific data used for tree traversal decisions
- `level`: Tree level indicator for items on the current page being processed
- `heapPtr`: ItemPointerData containing heap tuple location information
- `isNull`: Boolean flag indicating this item represents a NULL value
- `isLeaf`: Boolean flag indicating this item represents a heap tuple (leaf level)
- `recheck`: Boolean flag indicating search conditions need to be rechecked
- `recheckDistances`: Boolean flag indicating distance calculations need to be rechecked
- `distances[FLEXIBLE_ARRAY_MEMBER]`: Flexible array of distance values for ordered search operations
## Dependencies
- Functions called/Symbols referenced:
  - [pairingheap_node](../p/pairingheap_node.md) (priority queue implementation)
  - [SpGistLeafTuple](SpGistLeafTuple.md) (leaf tuple representation)
  - FLEXIBLE_ARRAY_MEMBER (variable-length array support)
  - Datum (PostgreSQL data type)
  - [ItemPointerData](../I/ItemPointerData.md) (heap tuple pointers)

- Called from (representative examples):
  - [spgAllocSearchItem](../s/spgAllocSearchItem.md) (spgscan.c:117)
  - [spgAddSearchItemToQueue](../s/spgAddSearchItemToQueue.md) (spgscan.c:108)
  - [spgNewHeapItem](../s/spgNewHeapItem.md) (spgscan.c:467)
  - [spgMakeInnerItem](../s/spgMakeInnerItem.md) (spgscan.c:630)
  - [spgGetNextQueueItem](../s/spgGetNextQueueItem.md) (spgscan.c:752)

## Notes and Other Information
- Central to SP-GiST's search algorithm, enabling efficient priority-based traversal
- Supports both exact match and nearest-neighbor search operations
- The flexible array member allows for multiple distance calculations in ordered queries
- Used extensively in the pairing heap-based search queue for optimal performance
- Handles complex rechecking scenarios where index conditions need heap tuple verification
- Critical for maintaining search state across multiple tree levels during scan operations
- Size calculation available through SizeOfSpGistSearchItem macro