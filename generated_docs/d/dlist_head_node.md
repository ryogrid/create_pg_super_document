# dlist_head_node

## Location
src/include/lib/ilist.h: 565 - 571

## Overview
Returns the first node in a doubly-linked list, providing direct access to the head node element when the list is guaranteed to contain at least one element.

## Definition


## Detailed Description
This function is a convenience wrapper around  that returns the first node in a doubly-linked list. It assumes the list contains at least one element and will return undefined behavior if called on an empty list. The function is implemented as a static inline for performance, calling  with a zero offset to get the actual node structure. This is part of PostgreSQL's intrusive doubly-linked list implementation where list nodes are embedded within the data structures they organize.

## Parameters / Member Variables
- : Pointer to the list head structure that contains metadata about the doubly-linked list

## Dependencies
- Functions called/Symbols referenced:
  - dlist_head_element_off
  - dlist_node (return type)
  - dlist_head (parameter type)
- Called from (representative examples):
  - dataPlaceToPageLeafSplit (src/backend/access/gin/gindatapage.c:1059)
  - leafRepackItems (src/backend/access/gin/gindatapage.c:1589)
  - pgstat_flush_pending_entries (src/backend/utils/activity/pgstat.c:1197)
  - BumpReset (src/backend/utils/mmgr/bump.c:270)
  - BumpAlloc (src/backend/utils/mmgr/bump.c:520)
  - GenerationReset (src/backend/utils/mmgr/generation.c:320)

## Notes and Other Information
- This function should only be called when the list is known to contain at least one element
- The function is defined as static inline for optimal performance
- Part of PostgreSQL's intrusive list implementation in src/include/lib/ilist.h
- Returns a pointer to the actual dlist_node structure, not the containing data structure