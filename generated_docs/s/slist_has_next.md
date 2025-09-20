# slist_has_next

## Location
[src/include/lib/ilist.h:1043-1053](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L1043-L1053)

## Overview
Checks whether a given node in a singly linked list has a following node in PostgreSQL's intrusive list implementation.

## Definition

```c
static inline bool
slist_has_next(const slist_head *head, const slist_node *node)
```
## Detailed Description
This function provides a safe way to determine if a node in a singly linked list has a subsequent node. It performs this check by examining the node's next pointer to see if it points to NULL (indicating the end of the list) or to another valid node. The function also validates the list integrity through slist_check() before performing the actual check.

The operation runs in O(1) constant time and is implemented as an inline function for optimal performance. This function is typically used in list traversal operations where you need to determine if it's safe to advance to the next node without reaching the end of the list.

## Parameters / Member Variables
- : Pointer to the list head structure (used for integrity checking)
- : Pointer to the node to check for a following node

## Dependencies
- Functions called/Symbols referenced:
  - [slist_check](slist_check.md) (for list integrity validation)
- Data types used:
  - [slist_head](slist_head.md) (const)
  - [slist_node](slist_node.md) (const)
- Called from (representative examples):
  - [slist_next_node](slist_next_node.md) (src/include/lib/ilist.h:1056)

## Notes and Other Information
- This is an inline function for maximum performance in list operations
- The function uses const qualifiers for both parameters, indicating it does not modify the list
- [List](../L/List.md) integrity is validated through slist_check() before checking the next pointer
- Returns true if the node has a following node, false if it's the last node in the list
- Part of PostgreSQL's intrusive list implementation that provides safe list traversal
- The function assumes both head and node pointers are valid (non-NULL)
- Primarily used as a building block for other list traversal functions like slist_next_node
- The head parameter is required even though the actual check only uses the node, because it enables list integrity validation