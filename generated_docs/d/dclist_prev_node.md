# dclist_prev_node

## Location
src/include/lib/ilist.h: 879 - 887

## Overview
Returns the previous node in a doubly-linked counted list, providing backward traversal capability with the assumption that a previous node exists.

## Definition
```c
static inline dlist_node *
dclist_prev_node(dclist_head *head, dlist_node *node)
```

## Detailed Description
The `dclist_prev_node` function retrieves the previous node in a doubly-linked counted list. This function is part of PostgreSQL's counted list infrastructure and serves as a wrapper around the underlying dlist implementation. It includes a safety assertion to ensure the list is not empty before attempting to access the previous node. The function assumes that a previous node exists and should only be called when this condition is verified.

## Parameters / Member Variables
- `head`: Pointer to the dclist_head structure representing the counted list
- `node`: Pointer to the current dlist_node from which to get the previous node

## Dependencies
- Functions called/Symbols referenced:
  - dlist_prev_node
- Called from (representative examples):
  - No direct callers found in codebase

## Notes and Other Information
- The function includes an assertion to ensure the list count is greater than zero
- This is an inline function for performance optimization
- The caller must ensure that a previous node exists before calling this function
- Located in src/include/lib/ilist.h:879-887
- Returns a pointer to the previous dlist_node in the sequence