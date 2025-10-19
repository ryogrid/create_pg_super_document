# dlist_tail_node

## Location
[src/include/lib/ilist.h:582-592](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L582-L592)

## Overview
Returns the last node in a doubly-linked list, providing direct access to the tail node element when the list is guaranteed to contain at least one element.

## Definition
```c
static inline dlist_node *
dlist_tail_node(dlist_head *head)
```

## Detailed Description
This function is a convenience wrapper around `dlist_tail_element_off` that returns the last node in a doubly-linked list. It assumes the list contains at least one element and will return undefined behavior if called on an empty list. The function is implemented as a static inline for performance, calling `dlist_tail_element_off` with a zero offset to get the actual node structure. This is part of PostgreSQL's intrusive doubly-linked list implementation where list nodes are embedded within the data structures they organize.

## Parameters / Member Variables
- `head`: Pointer to the list head structure that contains metadata about the doubly-linked list

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_tail_element_off](dlist_tail_element_off.md)
  - [dlist_node](dlist_node.md) (return type)
  - [dlist_head](dlist_head.md) (parameter type)
- Called from (representative examples):
  - [dataBeginPlaceToPageLeaf](dataBeginPlaceToPageLeaf.md) (src/backend/access/gin/gindatapage.c:502)

## Notes and Other Information
- This function should only be called when the list is known to contain at least one element
- The function is defined as static inline for optimal performance
- Part of PostgreSQL's intrusive list implementation in src/include/lib/ilist.h
- Returns a pointer to the actual dlist_node structure, not the containing data structure
- Companion function to dlist_head_node for accessing the opposite end of the list

## Simplified Source

```c
static inline dlist_node *
dlist_tail_node(dlist_head *head) {
    // Return the last node in the list (assumes list is not empty)
    return (dlist_node *) dlist_tail_element_off(head, 0);
}
```