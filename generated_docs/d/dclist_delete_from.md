# dclist_delete_from

## Location
[src/include/lib/ilist.h:763-775](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L763-L775)

## Overview
Deletes a specified node from a doubly-linked circular list while maintaining the accurate count of remaining elements in the list.

## Definition
```c
static inline void
dclist_delete_from(dclist_head *head, dlist_node *node)
```

## Detailed Description
The dclist_delete_from function provides a clean way to remove a specific node from a doubly-linked circular list while ensuring the element count remains accurate. The function assumes the caller has verified that the node to be deleted is actually a member of the specified list, and includes an assertion to ensure the list is not empty before attempting deletion.

The function delegates the actual node removal to the underlying dlist_delete_from implementation, then decrements the count to maintain consistency. Unlike some other dclist operations, this function does not include explicit member validation, placing the responsibility on the caller to ensure the node belongs to the list.

## Parameters / Member Variables
- `head`: Pointer to the dclist_head structure representing the circular list header and metadata
- `node`: Pointer to the dlist_node to be removed from the list (must be a member of the list)

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_delete_from](dlist_delete_from.md)
- Called from (representative examples):
  - [logical_heap_rewrite_flush_mappings](../l/logical_heap_rewrite_flush_mappings.md) (src/backend/access/heap/rewriteheap.c:866)
  - [mXactCachePut](../m/mXactCachePut.md) (src/backend/access/transam/multixact.c:1735)
  - [ReorderBufferCleanupTXN](../R/ReorderBufferCleanupTXN.md) (src/backend/replication/logical/reorderbuffer.c:1625)
  - AtEOXact_PgStat_DroppedStats (src/backend/utils/activity/pgstat_xact.c:100)
  - [InvalidateConstraintCacheCallBack](../I/InvalidateConstraintCacheCallBack.md) (src/backend/utils/adt/ri_triggers.c:2259)
  - [SlabReset](../S/SlabReset.md) (src/backend/utils/mmgr/slab.c:449)

## Notes and Other Information
- Requires the list to be non-empty (count > 0) before deletion can proceed
- Unlike insert operations, does not include explicit member validation - caller must ensure the node belongs to the list
- Maintains accurate element count by decrementing after successful deletion
- Implemented as a static inline function for performance efficiency
- Widely used across PostgreSQL subsystems for cleanup operations in replication, statistics, memory management, and constraint handling
- Part of PostgreSQL's intrusive list implementation that doesn't require separate memory allocation for list nodes
- Does not check for count underflow, relying on the assertion to catch empty list scenarios