# slist_head_element_off

## Location
src/include/lib/ilist.h: 1062 - 1071

## Overview
An internal support function that computes the address of the structure containing the head element of a singly-linked list, given an offset to the list node member within that structure.

## Definition


## Detailed Description
This function is a low-level utility used internally by the singly-linked list implementation to convert from a list node pointer to the containing structure pointer. It performs pointer arithmetic to calculate the address of the structure that contains the head element's list node. The function assumes the list is not empty and uses the provided offset to subtract from the node's address to get the containing structure's address.

## Parameters / Member Variables
- : Pointer to the singly-linked list head structure
- : Byte offset of the slist_node member within the containing structure

## Dependencies
- Functions called/Symbols referenced:
  - [slist_is_empty](slist_is_empty.md) (for assertion check)
  - [slist_head](slist_head.md) (structure type)
  - [slist_node](slist_node.md) (structure type)
- Called from (representative examples):
  - [slist_head_node](slist_head_node.md)
  - slist_head_element

## Notes and Other Information
- This is an internal support function marked as static inline for performance
- Contains an assertion to ensure the list is not empty before accessing the head element
- Uses pointer arithmetic to convert from node address to containing structure address
- Part of PostgreSQL's intrusive linked list implementation in src/include/lib/ilist.h
- The offset parameter is typically computed using offsetof() macro in calling functions