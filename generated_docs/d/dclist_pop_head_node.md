# dclist_pop_head_node

## Location
[src/include/lib/ilist.h:789-807](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L789-L807)

## Overview
Removes and returns the first node from a doubly-linked counted list, ensuring the list contains at least one element.

## Definition
```c
static inline dlist_node *dclist_pop_head_node(dclist_head *head)
```

## Detailed Description
This function removes the first (head) node from a doubly-linked counted list and returns a pointer to that node. It maintains the integrity of the counted list by decrementing the count after removal. The function includes an assertion to ensure the list is not empty before attempting to pop a node, preventing undefined behavior when called on empty lists.

This is a convenience function that combines node removal with count management, making it easier to work with counted lists without manually tracking the count.

## Parameters / Member Variables
- `head`: Pointer to the counted list head structure from which to pop the first node

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_pop_head_node](dlist_pop_head_node.md)
  - [dclist_head](dclist_head.md) (structure type)
  - [dlist_node](dlist_node.md) (structure type)
- Called from (representative examples):
  - [SlabAllocFromNewBlock](../S/SlabAllocFromNewBlock.md) (src/backend/utils/mmgr/slab.c:550)

## Notes and Other Information
- This is an inline function defined in the header file for performance
- Includes an assertion to verify the list count is greater than zero before popping
- The caller receives ownership of the popped node and is responsible for its lifecycle
- Primarily used in memory management scenarios, particularly in slab allocation
- The function assumes the list is non-empty; calling it on an empty list will trigger an assertion failure

## Simplified Source

```c
static inline dlist_node *
dclist_pop_head_node(dclist_head *head) {
    // Ensure list is not empty
    Assert(head->count > 0);

    // Remove and return the first node
    dlist_node *node = dlist_pop_head_node(&head->dlist);
    head->count--;

    return node;
}
```