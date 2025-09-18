# dlist_pop_head_node

## Location
src/include/lib/ilist.h: 450 - 466

## Overview
Removes and returns the first node from a doubly-linked list, providing a stack-like LIFO (Last In, First Out) operation for the head of the list.

## Definition
```c
static inline dlist_node *dlist_pop_head_node(dlist_head *head)
```

## Detailed Description
This function implements a pop operation that removes the first node from the head of a doubly-linked list and returns a pointer to that node. It assumes that the list is not empty and will assert if called on an empty list. The function first verifies that the list contains at least one element using `dlist_is_empty`, then retrieves the first node by accessing `head->head.next`, removes it from the list using `dlist_delete`, and returns the detached node.

This operation is commonly used in scenarios where the list is being used as a queue or stack data structure, allowing efficient removal of elements from the front of the list. The caller is responsible for managing the memory of the returned node.

## Parameters / Member Variables
- `head`: Pointer to the list head from which to pop the first node

## Dependencies
- Functions called/Symbols referenced:
  - dlist_is_empty
  - dlist_delete
  - dlist_head (type)
  - dlist_node (type)
- Called from (representative examples):
  - do_start_worker
  - ReorderBufferIterTXNNext
  - ReorderBufferIterTXNFinish
  - CreatePredXact
  - InitProcess
  - dclist_pop_head_node

## Notes and Other Information
- This is an inline function defined in the header file for performance
- The function asserts that the list is not empty - calling on an empty list will cause program termination
- The returned node is removed from the list but its memory is not freed - the caller must handle disposal
- This provides LIFO behavior when used with `dlist_push_head` operations
- The node's next/prev pointers are updated by `dlist_delete` but not nullified (use `dlist_delete_thoroughly` if you need pointer nullification)
- Commonly used in PostgreSQL's process management and replication systems for managing queues of work items