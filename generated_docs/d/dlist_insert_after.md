# dlist_insert_after

## Location
[src/include/lib/ilist.h:381-392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L381-L392)

## Overview
Inserts a new node after a specified node in a doubly-linked list, maintaining proper linkages in both directions.

## Definition


## Detailed Description
This function inserts a new node immediately after an existing node in a doubly-linked list. The function updates all necessary pointers to maintain the integrity of the doubly-linked list structure. It assumes that both nodes belong to the same list and that the 'after' node is already properly linked in the list.

The insertion process involves four pointer updates:
1. Set the new node's previous pointer to the 'after' node
2. Set the new node's next pointer to what was previously after the 'after' node
3. Update the 'after' node's next pointer to point to the new node
4. Update the next node's previous pointer to point back to the new node

## Parameters / Member Variables
- : Pointer to the existing node after which the new node will be inserted
- : Pointer to the new node to be inserted into the list

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_node](dlist_node.md) (data structure)
- Called from (representative examples):
  - [leafRepackItems](../l/leafRepackItems.md) (src/backend/access/gin/gindatapage.c:1638)
  - [SyncRepQueueInsert](../S/SyncRepQueueInsert.md) (src/backend/replication/syncrep.c:390)
  - [dclist_insert_after](dclist_insert_after.md) (src/include/lib/ilist.h:732)

## Notes and Other Information
- This is an inline function for performance optimization
- The function assumes both nodes are part of the same list and does not perform validation
- No null pointer checks are performed - caller must ensure valid pointers
- The function maintains the doubly-linked nature of the list by updating both forward and backward pointers
- Used extensively in PostgreSQL for maintaining various internal data structures like GIN index pages and synchronous replication queues