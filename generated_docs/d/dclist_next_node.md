# dclist_next_node

## Location
[src/include/lib/ilist.h:867-878](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L867-L878)

## Overview
Returns the next node in a doubly-linked counted list, providing forward traversal capability with the assumption that a next node exists.

## Definition
```c
static inline dlist_node *
dclist_next_node(dclist_head *head, dlist_node *node)
```

## Detailed Description
The `dclist_next_node` function retrieves the next node in a doubly-linked counted list. This function is part of PostgreSQL's counted list infrastructure and serves as a wrapper around the underlying dlist implementation. It includes a safety assertion to ensure the list is not empty before attempting to access the next node. The function assumes that a next node exists and should only be called when this condition is verified.

## Parameters / Member Variables
- `head`: Pointer to the dclist_head structure representing the counted list
- `node`: Pointer to the current dlist_node from which to get the next node

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_next_node](dlist_next_node.md)
- Called from (representative examples):
  - No direct callers found in codebase

## Notes and Other Information
- The function includes an assertion to ensure the list count is greater than zero
- This is an inline function for performance optimization
- The caller must ensure that a next node exists before calling this function
- Located in src/include/lib/ilist.h:867-878
- Returns a pointer to the next dlist_node in the sequence