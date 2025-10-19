# slist_head_element_off

## Location
[src/include/lib/ilist.h:1062-1071](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L1062-L1071)

## Overview
An internal support function that computes the address of the structure containing the head element of a singly-linked list, given an offset to the list node member within that structure.

## Definition

```c
static inline void *
slist_head_element_off(slist_head *head, size_t off)
```
## Detailed Description
This function is a low-level utility used internally by the singly-linked list implementation to convert from a list node pointer to the containing structure pointer. It performs pointer arithmetic to calculate the address of the structure that contains the head element's list node. The function assumes the list is not empty and uses the provided offset to subtract from the node's address to get the containing structure's address.

## Parameters / Member Variables
- `*head`: Pointer to the singly-linked list head structure
- `off`: Byte offset of the slist_node member within the containing structure
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

## Simplified Source

```c
static inline void *
slist_head_element_off(slist_head *head, size_t off)
{
    // Ensure list has at least one element
    Assert(!slist_is_empty(head));

    // Get containing struct address by subtracting offset from head node address
    return (char *) head->head.next - off;
}
```

**Key Points:**
- Internal utility for intrusive list implementation
- Uses pointer arithmetic: head_node_address - offset = containing_struct_address
- The `head.next` points to the first node in the singly-linked list
- Enables conversion from list node pointer to containing structure pointer