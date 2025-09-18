# dclist_insert_after

## Location
src/include/lib/ilist.h: 727 - 744

## Overview
Inserts a node after a specified existing node within the same doubly-linked circular list, with validation to ensure the reference node is actually a member of the target list.

## Definition
```c
static inline void
dclist_insert_after(dclist_head *head, dlist_node *after, dlist_node *node)
```

## Detailed Description
The dclist_insert_after function provides precise control over node placement within a doubly-linked circular list by inserting a new node immediately after a specified existing node. Unlike the push operations that insert at predetermined positions (head or tail), this function allows insertion at any position within the list.

The function includes important safety measures: it validates that the reference node ('after') is actually a member of the specified list using dlist_member_check, and ensures the list is not empty before attempting the insertion. After successfully inserting the node using the underlying dlist_insert_after implementation, it increments the count and validates for potential overflow.

## Parameters / Member Variables
- `head`: Pointer to the dclist_head structure representing the circular list header and metadata
- `after`: Pointer to the existing dlist_node after which the new node will be inserted (must be a member of the list)
- `node`: Pointer to the dlist_node to be inserted into the list

## Dependencies
- Functions called/Symbols referenced:
  - dlist_member_check
  - dlist_insert_after
- Called from (representative examples):
  - No references found in current codebase

## Notes and Other Information
- Includes comprehensive validation: the 'after' node must be verified as a member of the target list
- Requires the list to be non-empty (count > 0) before insertion can proceed
- Provides precise positioning control within the list structure
- Includes count overflow protection through assertion checking
- Implemented as a static inline function for performance efficiency
- Currently unused in the codebase, suggesting it may be part of a complete API intended for future use
- Part of PostgreSQL's intrusive list implementation that doesn't require separate memory allocation for list nodes