# dlist_has_prev

## Location
src/include/lib/ilist.h: 513 - 524

## Overview
Checks whether a given node in a doubly-linked list has a preceding node (i.e., is not the first node in the list).

## Definition


## Detailed Description
This function determines if a node has a previous element by comparing the node's prev pointer with the head sentinel's head field. In PostgreSQL's doubly-linked list implementation, the head sentinel's head field serves as a boundary marker - if a node's prev pointer points to this sentinel, it means the node is the first element in the list and has no actual preceding node.

The function is marked as static inline for performance, as it's a simple pointer comparison that benefits from inlining. However, it comes with an important caveat: the function assumes the node is actually part of the specified list and will produce unreliable results if the node is detached or belongs to a different list.

## Parameters / Member Variables
- : Pointer to the list head structure containing the sentinel node
- : Pointer to the node being checked for a preceding element

## Dependencies
- Functions called/Symbols referenced:
  - dlist_head (struct type)
  - dlist_node (struct type)
- Called from (representative examples):
  - dataBeginPlaceToPageLeaf (src/backend/access/gin/gindatapage.c:639)
  - dlist_prev_node (src/include/lib/ilist.h:549)
  - dclist_has_prev (src/include/lib/ilist.h:859)

## Notes and Other Information
- **Caution**: This function is unreliable if the node is not actually in the specified list
- The function performs a simple pointer comparison and does not validate list membership
- Used internally by other list navigation functions like dlist_prev_node
- Part of PostgreSQL's intrusive doubly-linked list implementation in ilist.h