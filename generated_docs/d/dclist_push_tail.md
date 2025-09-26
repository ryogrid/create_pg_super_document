# dclist_push_tail

## Location
[src/include/lib/ilist.h:709-726](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L709-L726)

## Overview
Inserts a node at the end of a doubly-linked circular list, automatically initializing the list if it was previously empty and maintaining an accurate count of elements.

## Definition
```c
static inline void
dclist_push_tail(dclist_head *head, dlist_node *node)
```

## Detailed Description
The dclist_push_tail function provides a convenient way to insert a new node at the tail (end) of a doubly-linked circular list while maintaining both the circular structure and an accurate element count. Similar to dclist_push_head, this function handles the special case where the list might be in a NULL state (uninitialized) by automatically converting it to a proper circular list structure before performing the insertion.

The function leverages the underlying dlist_push_tail implementation for the actual node insertion logic, then increments the count to maintain consistency. It includes an assertion to detect potential count overflow scenarios.

## Parameters / Member Variables
- `head`: Pointer to the dclist_head structure representing the circular list header and metadata
- `node`: Pointer to the dlist_node to be inserted at the end of the list

## Dependencies
- Functions called/Symbols referenced:
  - [dclist_init](dclist_init.md)
  - [dlist_push_tail](dlist_push_tail.md)
- Called from (representative examples):
  - [logical_rewrite_log_mapping](../l/logical_rewrite_log_mapping.md) (src/backend/access/heap/rewriteheap.c:983)
  - [ReorderBufferXidSetCatalogChanges](../R/ReorderBufferXidSetCatalogChanges.md) (src/backend/replication/logical/reorderbuffer.c:3540, 3556)
  - [DeadLockCheck](../D/DeadLockCheck.md) (src/backend/storage/lmgr/deadlock.c:262)
  - [ProcSleep](../P/ProcSleep.md) (src/backend/storage/lmgr/proc.c:1197)
  - [ri_LoadConstraintInfo](../r/ri_LoadConstraintInfo.md) (src/backend/utils/adt/ri_triggers.c:2182)

## Notes and Other Information
- The function automatically handles list initialization if the list header indicates a NULL state
- Includes count overflow protection through assertion checking
- Maintains the circular list property while providing counting functionality
- Implemented as a static inline function for performance efficiency
- Widely used across PostgreSQL for various subsystems including replication, deadlock detection, and statistics management
- Part of PostgreSQL's intrusive list implementation that doesn't require separate memory allocation for list nodes