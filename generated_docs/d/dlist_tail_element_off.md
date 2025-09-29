# dlist_tail_element_off

## Location
[src/include/lib/ilist.h:572-581](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L572-L581)

## Overview
Internal support function that calculates the address of the data structure containing the tail element of a doubly-linked list, using pointer arithmetic with the specified offset.

## Definition
```c
static inline void *
dlist_tail_element_off(dlist_head *head, size_t off)
```

## Detailed Description
This is an internal utility function that performs pointer arithmetic to calculate the address of the data structure containing the tail element of a doubly-linked list. It subtracts the specified offset from the previous pointer of the head node to get the address of the containing structure. The function includes an assertion to ensure the list is not empty before attempting the operation. This is part of PostgreSQL's intrusive list implementation where list nodes are embedded within larger data structures.

## Parameters / Member Variables
- `head`: Pointer to the list head structure containing metadata about the doubly-linked list
- `off`: Byte offset from the beginning of the containing structure to the embedded dlist_node

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_is_empty](dlist_is_empty.md) (for assertion check)
  - [dlist_head](dlist_head.md) (parameter type)
  - [dlist_node](dlist_node.md) (accessed via head->head.prev)
- Called from (representative examples):
  - [dlist_tail_node](dlist_tail_node.md) (src/include/lib/ilist.h:584)
  - dlist_tail_element (src/include/lib/ilist.h:614)
  - [dclist_tail_node](dclist_tail_node.md) (src/include/lib/ilist.h:924)

## Notes and Other Information
- This is an internal function not intended for direct use by application code
- Uses Assert() to verify the list is not empty before accessing the tail
- Performs pointer arithmetic: `(char *) head->head.prev - off`
- Part of the intrusive list implementation where nodes are embedded in data structures
- The function is static inline for performance optimization

## Simplified Source

```c
static inline void *
dlist_tail_element_off(dlist_head *head, size_t off)
{
    // Ensure list is not empty
    Assert(!dlist_is_empty(head));

    // Calculate address of containing structure
    return (char *) head->head.prev - off;
}
```