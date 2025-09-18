# dlist_head_element_off

## Location
src/include/lib/ilist.h: 555 - 564

## Overview
Internal support function that calculates the address of the structure containing the first element of a doubly-linked list, given an offset to the embedded dlist_node.

## Definition


## Detailed Description
This function is a low-level utility that implements the core logic for accessing the structure that contains the first dlist_node in a list. It works by taking the address of the first node (head->head.next) and subtracting the offset of the dlist_node field within the containing structure. This offset arithmetic allows the intrusive list implementation to retrieve the containing structure from just the embedded node pointer.

The function performs pointer arithmetic by casting to char* (to ensure byte-level arithmetic) and then subtracting the offset. This is a common technique in intrusive data structures where the list node is embedded within a larger structure, and you need to recover the containing structure's address.

An assertion ensures the list is not empty before attempting to access the first element, preventing undefined behavior when operating on empty lists.

## Parameters / Member Variables
- : Pointer to the list head structure containing the sentinel and list metadata
- : Byte offset of the dlist_node field within the containing structure (typically calculated using offsetof())

## Dependencies
- Functions called/Symbols referenced:
  - dlist_head (struct type)
  - dlist_is_empty (function to check if list is empty)
  - Assert (macro for debug assertions)
- Called from (representative examples):
  - dlist_head_node (src/include/lib/ilist.h:567)
  - dlist_head_element (src/include/lib/ilist.h:605)
  - dclist_head_node (src/include/lib/ilist.h:904)

## Notes and Other Information
- This is an internal implementation function not typically called directly by user code
- Used as the building block for higher-level macros like dlist_head_element() that provide type-safe access
- The offset parameter is typically calculated using the offsetof() macro
- Part of PostgreSQL's intrusive list implementation that embeds list nodes within data structures
- The function assumes the caller has properly calculated the offset and that the list contains elements of the expected type