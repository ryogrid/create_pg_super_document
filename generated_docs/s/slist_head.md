# slist_head

## Location
src/include/lib/ilist.h: 236 - 239

## Overview
The head structure for managing singly linked lists, providing the entry point and control structure for list operations.

## Definition


## Detailed Description
The  structure serves as the control structure for singly linked lists in PostgreSQL. Unlike doubly linked lists, singly linked lists are not circularly linked - when the list is empty, the head.next pointer is simply set to NULL. This design choice eliminates additional conditional branches in common list manipulation operations, improving performance.

The structure contains a single  member called  which acts as a sentinel node. This design provides a consistent interface for list operations and simplifies the implementation of insertion and deletion operations by avoiding special cases for empty lists.

The singly linked design is optimized for scenarios where forward-only traversal is sufficient and memory overhead needs to be minimized, making it ideal for many internal PostgreSQL data structures.

## Parameters / Member Variables
- : A sentinel slist_node that serves as the starting point for the linked list; its  pointer points to the first actual node in the list, or NULL if the list is empty

## Dependencies
- Functions called/Symbols referenced:
  - [slist_node](slist_node.md) (embedded as the head sentinel)
- Called from (representative examples):
  - [slist_init](slist_init.md) (initializes the list head)
  - [slist_push_head](slist_push_head.md) (adds nodes to the front of the list)
  - [slist_pop_head_node](slist_pop_head_node.md) (removes nodes from the front of the list)
  - [slist_is_empty](slist_is_empty.md) (checks if the list is empty)
  - Various PostgreSQL subsystems for managing collections

## Notes and Other Information
- The non-circular design simplifies list operations and reduces branching compared to circular implementations
- Used extensively throughout PostgreSQL for managing collections where insertion/deletion at the head is the primary operation
- The sentinel node design means the list head itself never changes address, only its contents
- Common usage includes event trigger management, catalog cache headers, and GUC (Grand Unified Configuration) hash entries
- Part of PostgreSQL's optimized intrusive list infrastructure for performance-critical code paths