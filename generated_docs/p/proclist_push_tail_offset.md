# proclist_push_tail_offset

## Location
[src/include/storage/proclist.h:87-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/proclist.h#L87-L114)

## Overview
Inserts a process at the end (tail) of a proclist using a specified offset to locate the proclist_node within the PGPROC structure.

## Definition
```c
static inline void proclist_push_tail_offset(proclist_head *list, int procno, size_t node_offset)
```

## Detailed Description
The `proclist_push_tail_offset` function adds a process to the end of a doubly-linked process list. It handles two scenarios: inserting into an empty list and inserting into a non-empty list. The function uses the provided offset to locate the specific proclist_node field within the PGPROC structure.

When inserting into an empty list, both head and tail pointers are set to the new process number, and the node's next and prev pointers are set to `INVALID_PROC_NUMBER`. When inserting into a non-empty list, the function updates the linkages to maintain the doubly-linked structure: the new node becomes the tail, its prev pointer points to the previous tail, and the previous tail's next pointer is updated to point forward to the new node.

The function includes several assertion checks to ensure list integrity and that the node being inserted is not already part of a list.

## Parameters / Member Variables
- `list`: A pointer to the proclist_head structure representing the list
- `procno`: The process number of the process to be inserted at the tail
- `node_offset`: The byte offset of the proclist_node field within the PGPROC structure

## Dependencies
- Functions called/Symbols referenced:
  - [proclist_node_get](proclist_node_get.md) (to retrieve proclist_node pointers)
  - `INVALID_PROC_NUMBER` (constant indicating an invalid process number)
  - `[proclist_head](proclist_head.md)` (data structure type)
  - `[proclist_node](proclist_node.md)` (data structure type)
- Called from (representative examples):
  - `proclist_push_tail` (src/include/storage/proclist.h:192)

## Notes and Other Information
- This is a static inline function defined in the header file for optimal performance
- The function maintains the doubly-linked list invariants and performs integrity checks via assertions
- Inserting at the tail is an O(1) operation
- The function assumes the process being inserted is not already in any proclist (asserted by checking next and prev are 0)
- The offset-based approach allows the same function to work with different proclist_node fields within PGPROC structures
- After insertion, the new process becomes the last process that will be accessed when iterating from the head
- This function is commonly used for FIFO (first-in-first-out) queuing behavior when combined with head removal operations

## Simplified Source

```c
static inline void
proclist_push_tail_offset(proclist_head *list, int procno, size_t node_offset)
{
    proclist_node *node = proclist_node_get(procno, node_offset);

    // Handle empty list case
    if (list->tail == INVALID_PROC_NUMBER) {
        // First node in empty list
        node->next = node->prev = INVALID_PROC_NUMBER;
        list->head = list->tail = procno;
    }
    else {
        // Add to end of non-empty list
        node->prev = list->tail;
        proclist_node_get(node->prev, node_offset)->next = procno;
        node->next = INVALID_PROC_NUMBER;
        list->tail = procno;
    }
}
```