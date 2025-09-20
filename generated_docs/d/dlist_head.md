# dlist_head

## Location
[src/include/lib/ilist.h:151-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L151-L161)

## Overview
The  structure serves as the head of a doubly-linked list in PostgreSQL's intrusive list implementation, providing an anchor point for circular doubly-linked lists.

## Definition

```c
typedef struct dlist_head
{
	/*
	 * head.next either points to the first element of the list; to &head if
	 * it's a circular empty list; or to NULL if empty and not circular.
	 *
	 * head.prev either points to the last element of the list; to &head if
	 * it's a circular empty list; or to NULL if empty and not circular.
	 */
	dlist_node	head;
} dlist_head;
```
## Detailed Description
The  structure acts as the sentinel node for PostgreSQL's doubly-linked list implementation. It contains a single  member that serves as both the entry point into the list and maintains the circular linking structure. 

Non-empty lists are internally circularly linked, meaning the last element's  pointer points back to the head, and the head's  pointer points to the last element. This circular design eliminates the need for conditional branches in most list manipulation operations, improving performance.

The structure supports both circular and non-circular empty list representations:
- Circular empty list: both  and  point to  itself
- Non-circular empty list: both  and  are NULL

## Parameters / Member Variables
- : A  structure that serves as the sentinel node for the list
  - : Points to the first element of the list, to  for circular empty lists, or NULL for non-circular empty lists
  - : Points to the last element of the list, to  for circular empty lists, or NULL for non-circular empty lists

## Dependencies
- Functions called/Symbols referenced:
  -  (embedded as the head sentinel)
- Called from (representative examples):
  -  (initializes the list head)
  -  (checks if list is empty)
  - / (list insertion operations)
  -  and related iteration macros
  - Numerous PostgreSQL subsystems including memory management, caching, process management, and replication

## Notes and Other Information
- The circular linking design is a key performance optimization in PostgreSQL's list implementation
- Used extensively throughout PostgreSQL for managing collections of related objects
- Supports both initialization patterns (circular and non-circular empty lists) for flexibility
- The sentinel node approach simplifies list manipulation code by eliminating special cases for empty lists
- Typically manipulated through a comprehensive set of inline functions and macros rather than direct member access