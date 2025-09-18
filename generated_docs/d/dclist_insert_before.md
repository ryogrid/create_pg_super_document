# dclist_insert_before

## Location
[src/include/lib/ilist.h:745-762](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L745-L762)

## Overview
Inserts a node before a specified existing node within the same doubly-linked circular list, with validation to ensure the reference node is actually a member of the target list.

## Definition
```c
static inline void
dclist_insert_before(dclist_head *head, dlist_node *before, dlist_node *node)
```

## Detailed Description
The dclist_insert_before function provides precise control over node placement within a doubly-linked circular list by inserting a new node immediately before a specified existing node. This function complements dclist_insert_after by allowing insertion at any position within the list with the new node positioned before the reference node.

Similar to dclist_insert_after, this function includes important safety measures: it validates that the reference node ('before') is actually a member of the specified list using dlist_member_check, and ensures the list is not empty before attempting the insertion. After successfully inserting the node using the underlying dlist_insert_before implementation, it increments the count and validates for potential overflow.

## Parameters / Member Variables
- `head`: Pointer to the dclist_head structure representing the circular list header and metadata
- `before`: Pointer to the existing dlist_node before which the new node will be inserted (must be a member of the list)
- `node`: Pointer to the dlist_node to be inserted into the list

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_member_check](dlist_member_check.md)
  - [dlist_insert_before](dlist_insert_before.md)
- Called from (representative examples):
  - ProcSleep (src/backend/storage/lmgr/proc.c:1195)

## Notes and Other Information
- Includes comprehensive validation: the 'before' node must be verified as a member of the target list
- Requires the list to be non-empty (count > 0) before insertion can proceed
- Provides precise positioning control within the list structure, complementing dclist_insert_after
- Includes count overflow protection through assertion checking
- Implemented as a static inline function for performance efficiency
- Used in process sleep management within PostgreSQL's lock manager for maintaining wait queues
- Part of PostgreSQL's intrusive list implementation that doesn't require separate memory allocation for list nodes