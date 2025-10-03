# dlist_move_tail

## Location
[src/include/lib/ilist.h:486-502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L486-L502)

## Overview
Moves an existing node from its current position within a doubly-linked list to the tail (last) position of the same list, implementing an efficient repositioning operation.

## Definition
```c
static inline void dlist_move_tail(dlist_head *head, dlist_node *node)
```

## Detailed Description
This function efficiently repositions a node that is already part of a doubly-linked list to become the last element (tail) of that same list. Similar to `dlist_move_head`, it includes an optimization that checks if the node is already at the tail position and returns immediately if so, avoiding unnecessary operations. If the node is not already at the tail, the function removes it from its current position using `dlist_delete` and then inserts it at the tail using `dlist_push_tail`.

The function includes a debug check at the end using `dlist_check` to verify list integrity after the operation. This operation is commonly used in cache management systems where items need to be moved to the end of a list, such as implementing Most Recently Used (MRU) policies or aging mechanisms.

## Parameters / Member Variables
- `head`: Pointer to the list head where the node should be moved to the tail
- `node`: Pointer to the existing node within the list that should be moved to the tail position

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_delete](dlist_delete.md)
  - [dlist_push_tail](dlist_push_tail.md)
  - [dlist_check](dlist_check.md)
  - [dlist_head](dlist_head.md) (type)
  - [dlist_node](dlist_node.md) (type)
- Called from (representative examples):
  - [cache_lookup](../c/cache_lookup.md)
  - [dclist_move_tail](dclist_move_tail.md)

## Notes and Other Information
- This is an inline function defined in the header file for performance
- The function assumes the node is already part of the specified list - undefined behavior occurs if this assumption is violated
- Includes a fast path optimization: if the node is already at the tail, no operation is performed
- After the operation, the specified node becomes the last element in the list
- Used in PostgreSQL's memoization and caching systems for implementing cache eviction policies
- The function performs integrity checking in debug builds via `dlist_check`
- This operation maintains the total number of nodes in the list - it's purely a repositioning operation
- Complementary to `dlist_move_head` for complete list reorganization capabilities

## Simplified Source

```c
static inline void
dlist_move_tail(dlist_head *head, dlist_node *node)
{
    // Fast path: if already at tail, do nothing
    if (head->head.prev == node)
        return;

    // Remove from current position and add to tail
    dlist_delete(node);
    dlist_push_tail(head, node);

    // Debug integrity check
    dlist_check(head);
}
```