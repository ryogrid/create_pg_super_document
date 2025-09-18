# dclist_head_element_off

## Location
[src/include/lib/ilist.h:888-899](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L888-L899)

## Overview
Internal support function that calculates the address of the head element's containing structure by applying an offset to the first node in a doubly-linked counted list.

## Definition
```c
static inline void *
dclist_head_element_off(dclist_head *head, size_t off)
```

## Detailed Description
The `dclist_head_element_off` function is an internal utility that computes the memory address of the structure containing the head element of a doubly-linked counted list. It works by taking the address of the first node's next pointer and subtracting a given offset to reach the beginning of the containing structure. This function is typically used in conjunction with container_of-style macros to safely navigate from list nodes back to their containing data structures.

## Parameters / Member Variables
- `head`: Pointer to the dclist_head structure representing the counted list
- `off`: Size offset representing the distance from the beginning of the containing structure to the embedded dlist_node

## Dependencies
- Functions called/Symbols referenced:
  - [dclist_is_empty](dclist_is_empty.md)
- Called from (representative examples):
  - dclist_head_element

## Notes and Other Information
- This is an internal support function not intended for direct use by client code
- The function includes an assertion to ensure the list is not empty before accessing the head element
- This is an inline function for performance optimization
- Located in src/include/lib/ilist.h:888-899
- Returns a void pointer that must be cast to the appropriate structure type
- Used as part of the container_of pattern for type-safe structure member access