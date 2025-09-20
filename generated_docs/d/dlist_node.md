# dlist_node

## Location
[src/include/lib/ilist.h:136-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L136-L137)

## Overview
The  structure represents a node in PostgreSQL's doubly-linked list implementation, designed to be embedded within other structures that need to participate in doubly-linked lists.

## Definition

```c
typedef struct dlist_node dlist_node;
```
## Detailed Description
The  is a fundamental building block of PostgreSQL's intrusive doubly-linked list implementation found in . This structure is designed to be embedded directly into other structures that need to be part of a doubly-linked list, rather than being used as a container for data. This intrusive design provides better memory locality and eliminates the need for separate allocation of list nodes.

The structure forms the backbone of a circular doubly-linked list where non-empty lists are internally circularly linked. This circular design eliminates the need for branches in the most common list manipulations, improving performance.

## Parameters / Member Variables
- : Pointer to the previous node in the doubly-linked list
- : Pointer to the next node in the doubly-linked list

## Dependencies
- Functions called/Symbols referenced:
  - (self-referential structure only)
- Called from (representative examples):
  -  (contains dlist_node as the list head)
  -  (uses dlist_node for iteration)
  -  (uses dlist_node for safe iteration)
  - Various list manipulation functions (dlist_push_head, dlist_push_tail, etc.)
  - Numerous PostgreSQL subsystems including memory management, transaction processing, and replication

## Notes and Other Information
- This is an intrusive list design where the list node is embedded directly in the containing structure
- Used extensively throughout PostgreSQL for efficient list management
- The circular linking design improves performance by eliminating conditional branches
- Typically accessed through macros and inline functions rather than directly
- Part of a comprehensive list manipulation API that includes functions for insertion, deletion, iteration, and list management