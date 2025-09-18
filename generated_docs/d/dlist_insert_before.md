# dlist_insert_before

## Location
[src/include/lib/ilist.h:393-404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L393-L404)

## Overview
Inserts a new node before a specified node in a doubly-linked list, maintaining proper linkages in both directions.

## Definition
```c
static inline void dlist_insert_before(dlist_node *before, dlist_node *node)
```

## Detailed Description
This function inserts a new node immediately before an existing node in a doubly-linked list. The function updates all necessary pointers to maintain the integrity of the doubly-linked list structure. It assumes that both nodes belong to the same list and that the 'before' node is already properly linked in the list.

The insertion process involves four pointer updates:
1. Set the new node's previous pointer to what was previously before the 'before' node
2. Set the new node's next pointer to the 'before' node
3. Update the 'before' node's previous pointer to point to the new node
4. Update the previous node's next pointer to point to the new node

## Parameters / Member Variables
- `before`: Pointer to the existing node before which the new node will be inserted
- `node`: Pointer to the new node to be inserted into the list

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_node](dlist_node.md) (data structure)
- Called from (representative examples):
  - [ReorderBufferTransferSnapToParent](../R/ReorderBufferTransferSnapToParent.md) (src/backend/replication/logical/reorderbuffer.c:1188)
  - [dclist_insert_before](dclist_insert_before.md) (src/include/lib/ilist.h:750)

## Notes and Other Information
- This is an inline function for performance optimization
- The function assumes both nodes are part of the same list and does not perform validation
- No null pointer checks are performed - caller must ensure valid pointers
- The function maintains the doubly-linked nature of the list by updating both forward and backward pointers
- Used in PostgreSQL's logical replication system for managing transaction snapshots and in doubly-linked circular lists
- Complementary to dlist_insert_after, providing insertion flexibility depending on the desired position relative to an existing node