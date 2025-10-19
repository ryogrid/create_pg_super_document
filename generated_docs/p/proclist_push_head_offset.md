# proclist_push_head_offset

## Location
[src/include/storage/proclist.h:59-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/proclist.h#L59-L86)

## Overview
Inserts a process at the beginning (head) of a proclist using a specified offset to locate the proclist_node within the PGPROC structure.

## Definition
```c
static inline void proclist_push_head_offset(proclist_head *list, int procno, size_t node_offset)
```

## Detailed Description
The `proclist_push_head_offset` function adds a process to the front of a doubly-linked process list. It handles two scenarios: inserting into an empty list and inserting into a non-empty list. The function uses the provided offset to locate the specific proclist_node field within the PGPROC structure.

When inserting into an empty list, both head and tail pointers are set to the new process number, and the node's next and prev pointers are set to `INVALID_PROC_NUMBER`. When inserting into a non-empty list, the function updates the linkages to maintain the doubly-linked structure: the new node becomes the head, its next pointer points to the previous head, and the previous head's prev pointer is updated to point back to the new node.

The function includes several assertion checks to ensure list integrity and that the node being inserted is not already part of a list.

## Parameters / Member Variables
- `list`: A pointer to the proclist_head structure representing the list
- `procno`: The process number of the process to be inserted at the head
- `node_offset`: The byte offset of the proclist_node field within the PGPROC structure

## Dependencies
- Functions called/Symbols referenced:
  - [proclist_node_get](proclist_node_get.md) (to retrieve proclist_node pointers)
  - `INVALID_PROC_NUMBER` (constant indicating an invalid process number)
  - `[proclist_head](proclist_head.md)` (data structure type)
  - `[proclist_node](proclist_node.md)` (data structure type)
- Called from (representative examples):
  - `proclist_push_head` (src/include/storage/proclist.h:190)

## Notes and Other Information
- This is a static inline function defined in the header file for optimal performance
- The function maintains the doubly-linked list invariants and performs integrity checks via assertions
- Inserting at the head is an O(1) operation
- The function assumes the process being inserted is not already in any proclist (asserted by checking next and prev are 0)
- The offset-based approach allows the same function to work with different proclist_node fields within PGPROC structures
- After insertion, the new process becomes the first process that will be accessed when iterating from the head

## Simplified Source

```c
static inline void proclist_push_head_offset(proclist_head *list, int procno, size_t node_offset) {
    // Get the node to insert
    proclist_node *node = proclist_node_get(procno, node_offset);

    Assert(node->next == 0 && node->prev == 0);

    if (list->head == INVALID_PROC_NUMBER) {
        // Empty list: set both head and tail to new process
        Assert(list->tail == INVALID_PROC_NUMBER);
        node->next = node->prev = INVALID_PROC_NUMBER;
        list->head = list->tail = procno;
    } else {
        // Non-empty list: insert at head
        Assert(list->tail != INVALID_PROC_NUMBER);
        node->next = list->head;
        proclist_node_get(node->next, node_offset)->prev = procno;
        node->prev = INVALID_PROC_NUMBER;
        list->head = procno;
    }
}
```