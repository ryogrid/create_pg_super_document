# dclist_tail_element_off

## Location
[src/include/lib/ilist.h:909-919](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L909-L919)

## Overview
Internal support function that returns the address of the tail element's containing structure in a doubly-linked circular list.

## Definition

```c
static inline void *
dclist_tail_element_off(dclist_head *head, size_t off)
```
## Detailed Description
This function is an internal utility that calculates the memory address of the structure containing the tail node of a doubly-linked circular list. It works by taking the address of the tail node (accessed through head->dlist.head.prev) and subtracting the specified offset to get back to the beginning of the containing structure. This is part of PostgreSQL's intrusive list implementation where list nodes are embedded within larger data structures.

## Parameters / Member Variables
- : Pointer to the doubly-linked circular list head structure
- : Byte offset of the dlist_node member within the containing structure

## Dependencies
- Functions called/Symbols referenced:
  - [dclist_is_empty](dclist_is_empty.md) (validation check)
  - [dclist_head](dclist_head.md) (parameter type)
  - [dlist_node](dlist_node.md) (accessed through head->dlist.head.prev)
- Called from (representative examples):
  - dclist_tail_element (macro wrapper)

## Notes and Other Information
- This is an internal function marked as static inline for performance
- Includes an assertion to ensure the list is not empty before accessing the tail
- Uses pointer arithmetic to convert from node address to containing structure address
- Part of PostgreSQL's efficient intrusive list implementation where list nodes are embedded in data structures rather than allocated separately