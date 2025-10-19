# dclist_move_tail

## Location
[src/include/lib/ilist.h:824-838](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L824-L838)

## Overview
Moves a node from its current position within a doubly-linked counted list to the tail (last) position of the same list.

## Definition
```c
static inline void dclist_move_tail(dclist_head *head, dlist_node *node)
```

## Detailed Description
This function relocates an existing node within a doubly-linked counted list to the tail position without affecting the list's count. The node must already be a member of the specified list. The function includes membership verification and assertion checks to ensure the operation is valid.

This operation is useful in algorithms where recently accessed items need to be moved to the end of the list, such as in FIFO (First In, First Out) cache implementations or when implementing aging mechanisms where older items are moved toward the tail.

## Parameters / Member Variables
- `head`: Pointer to the counted list head structure containing the node
- `node`: Pointer to the existing list node to be moved to the tail position

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_member_check](dlist_member_check.md)
  - [dlist_move_tail](dlist_move_tail.md)
  - [dclist_head](dclist_head.md) (structure type)
  - [dlist_node](dlist_node.md) (structure type)
- Called from (representative examples):
  - No direct references found in the current codebase

## Notes and Other Information
- This is an inline function defined in the header file for performance
- Includes membership verification via dlist_member_check to ensure the node belongs to the list
- The list count remains unchanged since this is a reordering operation, not insertion/deletion
- Currently appears to be unused in the codebase but provides symmetry with dclist_move_head
- Caution: The node must already be a member of the specified list before calling this function
- Useful for implementing various list management strategies and cache replacement algorithms

## Simplified Source

```c
static inline void
dclist_move_tail(dclist_head *head, dlist_node *node) {
    // Verify that node is actually in this list
    dlist_member_check(&head->dlist, node);
    Assert(head->count > 0);  // List must not be empty

    // Move the node to the tail position
    dlist_move_tail(&head->dlist, node);
    // Note: Count remains unchanged since this is a move operation
}
```