# dclist_has_next

## Location
[src/include/lib/ilist.h:839-853](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L839-L853)

## Overview
Checks whether a given node in a doubly-linked counted list has a following (next) node, returning true if there is a subsequent node.

## Definition
```c
static inline bool dclist_has_next(const dclist_head *head, const dlist_node *node)
```

## Detailed Description
This function determines whether a specified node within a doubly-linked counted list has a following node. It performs membership verification to ensure the node belongs to the specified list and includes assertion checks for list validity. The function returns true if there is a node following the specified node, or false if the specified node is the last node in the list.

This function is useful for list traversal operations where you need to check if iteration can continue, or for algorithms that need to know about the position of nodes within the list structure.

## Parameters / Member Variables
- `head`: Pointer to the counted list head structure (const, not modified)
- `node`: Pointer to the list node to check for a following node (const, not modified)

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_member_check](dlist_member_check.md)
  - [dlist_has_next](dlist_has_next.md)
  - [dclist_head](dclist_head.md) (structure type)
  - [dlist_node](dlist_node.md) (structure type)
- Called from (representative examples):
  - No direct references found in the current codebase

## Notes and Other Information
- This is an inline function defined in the header file for performance
- Both parameters are const, indicating this is a read-only operation
- Includes membership verification via dlist_member_check to ensure the node belongs to the list
- Returns a boolean value: true if there is a next node, false otherwise
- Currently appears to be unused in the codebase but provides essential functionality for list iteration
- Caution: The node must already be a member of the specified list before calling this function
- Useful for implementing safe list traversal patterns and boundary checking

## Simplified Source

```c
static inline bool
dclist_has_next(const dclist_head *head, const dlist_node *node) {
    // Verify that node is actually in this list
    dlist_member_check(&head->dlist, node);
    Assert(head->count > 0);  // List must not be empty

    // Check if there's a following node
    return dlist_has_next(&head->dlist, node);
}
```