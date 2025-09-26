# dlist_head_node

## Location
[src/include/lib/ilist.h:565-571](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L565-L571)

## Overview
Returns the first node in a doubly-linked list, providing direct access to the head node element when the list is guaranteed to contain at least one element.

## Definition

```c
static inline dlist_node *
dlist_head_node(dlist_head *head)
```
## Detailed Description
This function is a convenience wrapper around  that returns the first node in a doubly-linked list. It assumes the list contains at least one element and will return undefined behavior if called on an empty list. The function is implemented as a static inline for performance, calling  with a zero offset to get the actual node structure. This is part of PostgreSQL's intrusive doubly-linked list implementation where list nodes are embedded within the data structures they organize.

## Parameters / Member Variables
- : Pointer to the list head structure that contains metadata about the doubly-linked list

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_head_element_off](dlist_head_element_off.md)
  - [dlist_node](dlist_node.md) (return type)
  - [dlist_head](dlist_head.md) (parameter type)
- Called from (representative examples):
  - [dataPlaceToPageLeafSplit](dataPlaceToPageLeafSplit.md) (src/backend/access/gin/gindatapage.c:1059)
  - [leafRepackItems](../l/leafRepackItems.md) (src/backend/access/gin/gindatapage.c:1589)
  - [pgstat_flush_pending_entries](../p/pgstat_flush_pending_entries.md) (src/backend/utils/activity/pgstat.c:1197)
  - [BumpReset](../B/BumpReset.md) (src/backend/utils/mmgr/bump.c:270)
  - [BumpAlloc](../B/BumpAlloc.md) (src/backend/utils/mmgr/bump.c:520)
  - [GenerationReset](../G/GenerationReset.md) (src/backend/utils/mmgr/generation.c:320)

## Notes and Other Information
- This function should only be called when the list is known to contain at least one element
- The function is defined as static inline for optimal performance
- Part of PostgreSQL's intrusive list implementation in src/include/lib/ilist.h
- Returns a pointer to the actual dlist_node structure, not the containing data structure