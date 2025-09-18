# dclist_head_node

## Location
src/include/lib/ilist.h: 900 - 908

## Overview
Returns the first node in a doubly-linked counted list, providing access to the head element with the assumption that the list contains at least one node.

## Definition
```c
static inline dlist_node *
dclist_head_node(dclist_head *head)
```

## Detailed Description
The `dclist_head_node` function retrieves the first node in a doubly-linked counted list. This function serves as a safe way to access the head node while ensuring the list is not empty through an assertion check. It leverages the underlying dlist implementation by calling `dlist_head_element_off` with a zero offset to get the first node directly, since the dlist_node itself is what we want to return rather than a containing structure.

## Parameters / Member Variables
- `head`: Pointer to the dclist_head structure representing the counted list

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_head_element_off](dlist_head_element_off.md)
- Called from (representative examples):
  - No direct callers found in codebase

## Notes and Other Information
- The function includes an assertion to ensure the list count is greater than zero
- This is an inline function for performance optimization  
- The caller must ensure that the list contains at least one element before calling this function
- Located in src/include/lib/ilist.h:900-908
- Returns a pointer to the first dlist_node in the list
- Uses dlist_head_element_off with offset 0 since we want the node itself, not a containing structure