# dclist_count

## Location
[src/include/lib/ilist.h:932-946](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L932-L946)

## Overview
Returns the stored number of entries in a doubly-linked circular list with constant-time complexity.

## Definition
```c
static inline uint32
dclist_count(const dclist_head *head)
```

## Detailed Description
This function provides efficient O(1) access to the number of elements in a doubly-linked circular list by returning the precomputed count stored in the list head structure. The function includes a consistency check assertion that verifies the stored count matches the actual list state (empty vs non-empty). This is part of PostgreSQL's counted list implementation that maintains an accurate element count to avoid expensive list traversals.

## Parameters / Member Variables
- `head`: Pointer to the doubly-linked circular list head structure (const-qualified)

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_is_empty](dlist_is_empty.md) (consistency validation)
  - [dclist_head](dclist_head.md) (parameter type)
- Called from (representative examples):
  - [logical_heap_rewrite_flush_mappings](../l/logical_heap_rewrite_flush_mappings.md) (heap rewriting)
  - [mXactCachePut](../m/mXactCachePut.md) (multixact transaction management)
  - [ReorderBufferGetCatalogChangesXacts](../R/ReorderBufferGetCatalogChangesXacts.md) (logical replication)
  - [DeadLockCheck](../D/DeadLockCheck.md) (deadlock detection)
  - [SlabStats](../S/SlabStats.md) (memory management)

## Notes and Other Information
- This is a static inline function for optimal performance
- Provides O(1) constant-time complexity for count operations
- Includes assertion to verify consistency between stored count and list empty state
- The const parameter indicates this is a read-only operation
- Widely used throughout PostgreSQL for efficient list size queries
- Essential for algorithms that need to know list size without traversing the entire list