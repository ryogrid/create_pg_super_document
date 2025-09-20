# slist_insert_after

## Location
[src/include/lib/ilist.h:1018-1027](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L1018-L1027)

## Overview
Inserts a node after a specified node in a singly linked list within PostgreSQL's intrusive list implementation.

## Definition

```c
static inline void
slist_insert_after(slist_node *after, slist_node *node)
```
## Detailed Description
This function provides the capability to insert a new node at a specific position within a singly linked list by placing it immediately after a given reference node. The operation is performed by first setting the new node's next pointer to point to whatever the reference node was pointing to, then updating the reference node's next pointer to point to the new node. This maintains the chain of the linked list while inserting the new element in the desired position.

The function operates in O(1) constant time and is implemented as an inline function for optimal performance. It's important to note that both nodes must belong to the same list for the operation to maintain list integrity, as indicated by the comment in the source code.

## Parameters / Member Variables
- : Pointer to the existing node after which the new node will be inserted
- : Pointer to the new node to be inserted into the list

## Dependencies
- Functions called/Symbols referenced:
  - [slist_node](slist_node.md) (data type used for both parameters)
- Called from (representative examples):
  - No direct references found in the current codebase

## Notes and Other Information
- This is an inline function for maximum performance in list operations
- Both nodes must be part of the same list for the operation to be valid
- The function does not perform any validation checks, unlike slist_push_head
- The caller is responsible for ensuring that the 'after' node is actually part of the target list
- This operation does not require access to the list head, making it efficient for mid-list insertions
- The function assumes that neither node is NULL and that the 'after' node is properly initialized
- Part of PostgreSQL's intrusive list implementation that embeds list nodes within data structures