# dlist_delete_from_thoroughly

## Location
[src/include/lib/ilist.h:440-449](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L440-L449)

## Overview
Safely removes a node from a doubly-linked list while performing thorough cleanup by setting the node's next/prev pointers to NULL to signal that it is no longer part of any list.

## Definition

```c
static inline void
dlist_delete_from_thoroughly(dlist_head *head, dlist_node *node)
```
## Detailed Description
This function provides a safe way to remove a node from a doubly-linked list with comprehensive validation and cleanup. It combines the safety checks of membership verification with thorough deletion that nullifies the node's pointers. The function first validates that the specified node is actually a member of the given list using , then performs the deletion using . This ensures both correctness (the node must belong to the list) and completeness (the node is fully disconnected and marked as unlinked).

The "thoroughly" aspect refers to the fact that after deletion, the node's next and prev pointers are set to NULL, which serves as a clear indicator that the node is not currently part of any list. This is particularly useful for debugging and preventing accidental reuse of nodes that have been removed from lists.

## Parameters / Member Variables
- : Pointer to the list head from which the node should be removed
- : Pointer to the node to be removed from the list

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_member_check](dlist_member_check.md)
  - [dlist_delete_thoroughly](dlist_delete_thoroughly.md)
  - [dlist_head](dlist_head.md) (type)
  - [dlist_node](dlist_node.md) (type)
- Called from (representative examples):
  - [dclist_delete_from_thoroughly](dclist_delete_from_thoroughly.md)

## Notes and Other Information
- This is an inline function defined in the header file for performance
- Provides both safety (membership validation) and thoroughness (pointer nullification)
- The function will assert/abort if the node is not actually a member of the specified list
- After this operation, the node's next/prev pointers will be NULL, making it safe to check if a node is currently in any list
- This is the safest way to remove a node when you want both validation and complete cleanup