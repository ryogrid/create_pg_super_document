# dlist_has_prev

## Location
[src/include/lib/ilist.h:513-524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L513-L524)

## Overview
Checks whether a given node in a doubly-linked list has a preceding node (i.e., is not the first node in the list).

## Definition

```c
static inline bool
dlist_has_prev(const dlist_head *head, const dlist_node *node)
```
## Detailed Description
This function determines if a node has a previous element by comparing the node's prev pointer with the head sentinel's head field. In PostgreSQL's doubly-linked list implementation, the head sentinel's head field serves as a boundary marker - if a node's prev pointer points to this sentinel, it means the node is the first element in the list and has no actual preceding node.

The function is marked as static inline for performance, as it's a simple pointer comparison that benefits from inlining. However, it comes with an important caveat: the function assumes the node is actually part of the specified list and will produce unreliable results if the node is detached or belongs to a different list.

## Parameters / Member Variables
- : Pointer to the list head structure containing the sentinel node
- : Pointer to the node being checked for a preceding element

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_head](dlist_head.md) (struct type)
  - [dlist_node](dlist_node.md) (struct type)
- Called from (representative examples):
  - [dataBeginPlaceToPageLeaf](dataBeginPlaceToPageLeaf.md) (src/backend/access/gin/gindatapage.c:639)
  - [dlist_prev_node](dlist_prev_node.md) (src/include/lib/ilist.h:549)
  - [dclist_has_prev](dclist_has_prev.md) (src/include/lib/ilist.h:859)

## Notes and Other Information
- **Caution**: This function is unreliable if the node is not actually in the specified list
- The function performs a simple pointer comparison and does not validate list membership
- Used internally by other list navigation functions like dlist_prev_node
- Part of PostgreSQL's intrusive doubly-linked list implementation in ilist.h