# dlist_delete

## Location
[src/include/lib/ilist.h:405-415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L405-L415)

## Overview
Removes a node from its doubly-linked list by updating the neighboring nodes' pointers to bypass the removed node.

## Definition
```c
static inline void dlist_delete(dlist_node *node)
```

## Detailed Description
This function removes a specified node from its doubly-linked list by updating the forward and backward pointers of the adjacent nodes. The function assumes the node is currently part of a valid doubly-linked list. After deletion, the node is effectively removed from the list, but its own pointers are not modified, leaving them pointing to their former neighbors.

The deletion process involves two pointer updates:
1. Update the previous node's next pointer to point to the node after the one being deleted
2. Update the next node's previous pointer to point to the node before the one being deleted

This effectively removes the node from the chain while maintaining list integrity.

## Parameters / Member Variables
- `node`: Pointer to the node to be removed from the list

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_node](dlist_node.md) (data structure)
- Called from (representative examples):
  - [leafRepackItems](../l/leafRepackItems.md) (src/backend/access/gin/gindatapage.c:1671, 1716, 1717)
  - [DestroyParallelContext](../D/DestroyParallelContext.md) (src/backend/access/transam/parallel.c:956)
  - [XLogPrefetcherAddFilter](../X/XLogPrefetcherAddFilter.md) (src/backend/access/transam/xlogprefetcher.c:883)
  - [CleanupBackgroundWorker](../C/CleanupBackgroundWorker.md) (src/backend/postmaster/postmaster.c:2755)
  - [ReorderBufferAssignChild](../R/ReorderBufferAssignChild.md) (src/backend/replication/logical/reorderbuffer.c:1120)
  - [LockAcquireExtended](../L/LockAcquireExtended.md) (src/backend/storage/lmgr/lock.c:1105, 1106)
  - [ReleasePredXact](../R/ReleasePredXact.md) (src/backend/storage/lmgr/predicate.c:600)
  - [CatCacheRemoveCTup](../C/CatCacheRemoveCTup.md) (src/backend/utils/cache/catcache.c:546)
  - [dlist_delete_from](dlist_delete_from.md) (src/include/lib/ilist.h:432)
  - [dlist_pop_head_node](dlist_pop_head_node.md) (src/include/lib/ilist.h:456)

## Notes and Other Information
- This is an inline function for performance optimization
- The function does NOT modify the deleted node's own pointers - they remain pointing to their former neighbors
- No null pointer checks are performed - caller must ensure the node pointer is valid
- The node must be part of a valid doubly-linked list when this function is called
- After deletion, the node's memory is not freed - that is the caller's responsibility
- Extensively used throughout PostgreSQL for managing various data structures including GIN indexes, parallel processing contexts, lock management, cache systems, and memory management
- The function does not perform any validation to ensure the node is actually in a list
- For safer deletion that also clears the node's pointers, use dlist_delete_thoroughly instead