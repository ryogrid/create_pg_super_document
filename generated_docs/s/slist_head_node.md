# slist_head_node

## Location
[src/include/lib/ilist.h:1072-1083](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L1072-L1083)

## Overview
Returns a pointer to the first node in a singly-linked list, assuming the list contains at least one element.

## Definition
```c
static inline slist_node *
slist_head_node(slist_head *head)
```

## Detailed Description
This function provides access to the first node in a singly-linked list by returning a pointer to the slist_node structure itself. It serves as a wrapper around slist_head_element_off with an offset of 0, effectively returning the address of the first node without any offset calculation. The function assumes the list is not empty and should only be called when the presence of at least one element has been verified.

## Parameters / Member Variables
- `head`: Pointer to the singly-linked list head structure

## Dependencies
- Functions called/Symbols referenced:
  - [slist_head_element_off](slist_head_element_off.md) (with offset 0)
  - [slist_head](slist_head.md) (structure type)
  - [slist_node](slist_node.md) (structure type)
- Called from (representative examples):
  - No direct references found in the current codebase

## Notes and Other Information
- This is a static inline function for performance optimization
- Uses offset 0 with slist_head_element_off since we want the node itself, not a containing structure
- Part of PostgreSQL's intrusive linked list implementation in src/include/lib/ilist.h
- Should only be called on non-empty lists; caller is responsible for checking list emptiness
- Returns the actual slist_node pointer, not the containing structure like other slist functions