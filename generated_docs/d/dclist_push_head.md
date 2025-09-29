# dclist_push_head

## Location
[src/include/lib/ilist.h:693-708](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L693-L708)

## Overview
Inserts a node at the beginning of a doubly-linked circular list, automatically initializing the list if it was previously empty and maintaining an accurate count of elements.

## Definition

```c
static inline void
dclist_push_head(dclist_head *head, dlist_node *node)
```
## Detailed Description
The dclist_push_head function provides a convenient way to insert a new node at the head (beginning) of a doubly-linked circular list while maintaining both the circular structure and an accurate element count. The function handles the special case where the list might be in a NULL state (uninitialized) by automatically converting it to a proper circular list structure before performing the insertion.

The function leverages the underlying dlist_push_head implementation for the actual node insertion logic, then increments the count to maintain consistency. It includes an assertion to detect potential count overflow scenarios.

## Parameters / Member Variables
- : Pointer to the dclist_head structure representing the circular list header and metadata
- : Pointer to the dlist_node to be inserted at the beginning of the list

## Dependencies
- Functions called/Symbols referenced:
  - [dclist_init](dclist_init.md)
  - [dlist_push_head](dlist_push_head.md)
- Called from (representative examples):
  - [mXactCachePut](../m/mXactCachePut.md) (src/backend/access/transam/multixact.c:1729)
  - [SlabFree](../S/SlabFree.md) (src/backend/utils/mmgr/slab.c:788)

## Notes and Other Information
- The function automatically handles list initialization if the list header indicates a NULL state
- Includes count overflow protection through assertion checking
- Maintains the circular list property while providing counting functionality
- Implemented as a static inline function for performance efficiency
- Part of PostgreSQL's intrusive list implementation that doesn't require separate memory allocation for list nodes

## Simplified Source

```c
static inline void dclist_push_head(dclist_head *head, dlist_node *node) {
    // Initialize list if empty
    if (head->dlist.head.next == NULL) {
        dclist_init(head);
    }

    // Add node to head and increment count
    dlist_push_head(&head->dlist, node);
    head->count++;

    Assert(head->count > 0); // Check for overflow
}
```