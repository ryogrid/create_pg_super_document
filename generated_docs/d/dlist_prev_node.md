# dlist_prev_node

## Location
[src/include/lib/ilist.h:547-554](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L547-L554)

## Overview
Returns the previous node in a doubly-linked list, with an assertion to ensure that a previous node actually exists.

## Definition

```c
static inline dlist_node *
dlist_prev_node(dlist_head *head, dlist_node *node)
```
## Detailed Description
This function provides safe backward navigation in a doubly-linked list by returning the node's prev pointer. Before returning the pointer, it uses an assertion to verify that a previous node actually exists by calling dlist_has_prev(). This prevents accidental traversal beyond the beginning of the list, which could lead to accessing the sentinel node or invalid memory.

The function is implemented as a static inline function for performance reasons, as list traversal operations are common and benefit from the elimination of function call overhead. The assertion serves as both documentation (indicating the precondition) and a runtime safety check in debug builds.

This function is the backward navigation counterpart to dlist_next_node, providing symmetric functionality for bidirectional list traversal.

## Parameters / Member Variables
- `*head`: Pointer to the list head structure, used by the assertion to verify that a previous node exists
- `*node`: Pointer to the current node whose previous node should be returned
## Dependencies
- Functions called/Symbols referenced:
  - [dlist_head](dlist_head.md) (struct type)
  - [dlist_node](dlist_node.md) (struct type)
  - [dlist_has_prev](dlist_has_prev.md) (function to verify previous node exists)
  - Assert (macro for debug assertions)
- Called from (representative examples):
  - [dataBeginPlaceToPageLeaf](dataBeginPlaceToPageLeaf.md) (src/backend/access/gin/gindatapage.c:665)
  - [leafRepackItems](../l/leafRepackItems.md) (src/backend/access/gin/gindatapage.c:1699)
  - [dclist_prev_node](dclist_prev_node.md) (src/include/lib/ilist.h:883)

## Notes and Other Information
- The function assumes that the caller has verified a previous node exists, enforced by the assertion
- Used in PostgreSQL's GIN index operations for backward list traversal
- Part of the safe list traversal API that prevents common list iteration errors
- Less frequently used than dlist_next_node, as forward traversal is more common than backward traversal
- The assertion helps catch programming errors where code attempts to traverse beyond list boundaries