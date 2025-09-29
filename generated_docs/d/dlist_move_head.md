# dlist_move_head

## Location
[src/include/lib/ilist.h:467-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L467-L485)

## Overview
Moves an existing node from its current position within a doubly-linked list to the head (first) position of the same list, implementing an efficient repositioning operation.

## Definition
```c
static inline void dlist_move_head(dlist_head *head, dlist_node *node)
```

## Detailed Description
This function efficiently repositions a node that is already part of a doubly-linked list to become the first element (head) of that same list. It includes an optimization that checks if the node is already at the head position and returns immediately if so, avoiding unnecessary work. If the node is not already at the head, the function removes it from its current position using `dlist_delete` and then inserts it at the head using `dlist_push_head`.

The function includes a debug check at the end using `dlist_check` to verify list integrity after the operation. This is commonly used in cache management systems where recently accessed items need to be moved to the front of a least-recently-used (LRU) list.

## Parameters / Member Variables
- `head`: Pointer to the list head where the node should be moved to the front
- `node`: Pointer to the existing node within the list that should be moved to the head position

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_delete](dlist_delete.md)
  - [dlist_push_head](dlist_push_head.md)
  - [dlist_check](dlist_check.md)
  - [dlist_head](dlist_head.md) (type)
  - [dlist_node](dlist_node.md) (type)
- Called from (representative examples):
  - [launch_worker](../l/launch_worker.md)
  - [SearchCatCacheInternal](../S/SearchCatCacheInternal.md)
  - [SearchCatCacheList](../S/SearchCatCacheList.md)
  - [dclist_move_head](dclist_move_head.md)

## Notes and Other Information
- This is an inline function defined in the header file for performance
- The function assumes the node is already part of the specified list - undefined behavior occurs if this assumption is violated
- Includes a fast path optimization: if the node is already at the head, no operation is performed
- After the operation, the specified node becomes the first element in the list
- Commonly used in PostgreSQL's catalog cache system for implementing LRU (Least Recently Used) behavior
- The function performs integrity checking in debug builds via `dlist_check`
- This operation maintains the total number of nodes in the list - it's purely a repositioning operation

## Simplified Source

```c
static inline void
dlist_move_head(dlist_head *head, dlist_node *node)
{
    // Fast path: already at head, nothing to do
    if (head->head.next == node)
        return;

    // Remove node from current position
    dlist_delete(node);

    // Insert at head position
    dlist_push_head(head, node);

    // Verify list integrity (debug builds)
    dlist_check(head);
}
```