# dclist_tail_node

## Location
src/include/lib/ilist.h: 920 - 931

## Overview
Returns a pointer to the last node in a doubly-linked circular list, with the requirement that the list contains at least one element.

## Definition
```c
static inline dlist_node *
dclist_tail_node(dclist_head *head)
```

## Detailed Description
This function provides direct access to the tail (last) node of a doubly-linked circular list. It leverages the underlying dlist implementation by calling dlist_tail_element_off with a zero offset, effectively returning the node itself rather than a containing structure. The function includes an assertion to ensure the list has at least one element before attempting to access the tail node.

## Parameters / Member Variables
- `head`: Pointer to the doubly-linked circular list head structure

## Dependencies
- Functions called/Symbols referenced:
  - dlist_tail_element_off (underlying list operation)
  - dlist_node (return type)
  - dclist_head (parameter type)
- Called from (representative examples):
  - mXactCachePut (multixact transaction management)

## Notes and Other Information
- This is a static inline function for optimal performance
- Includes an assertion (head->count > 0) to ensure the list is not empty
- Returns the actual node pointer, not the containing structure
- Part of PostgreSQL's counted doubly-linked list implementation
- The zero offset parameter to dlist_tail_element_off indicates we want the node itself rather than a containing structure