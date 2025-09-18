# dlist_delete_thoroughly

## Location
[src/include/lib/ilist.h:416-428](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L416-L428)

## Overview
Removes a node from its doubly-linked list and additionally sets the node's pointers to NULL to clearly indicate it is no longer part of any list.

## Definition
```c
static inline void dlist_delete_thoroughly(dlist_node *node)
```

## Detailed Description
This function provides a safer version of dlist_delete by not only removing the node from its doubly-linked list but also clearing the node's own next and prev pointers by setting them to NULL. This makes it clear that the node is no longer part of any list and helps prevent accidental use of stale pointers.

The thorough deletion process involves four operations:
1. Update the previous node's next pointer to point to the node after the one being deleted
2. Update the next node's previous pointer to point to the node before the one being deleted
3. Set the deleted node's next pointer to NULL
4. Set the deleted node's prev pointer to NULL

This approach provides better safety and debugging capabilities compared to the basic dlist_delete function.

## Parameters / Member Variables
- `node`: Pointer to the node to be removed from the list and thoroughly cleaned

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_node](dlist_node.md) (data structure)
- Called from (representative examples):
  - [SyncRepCancelWait](../S/SyncRepCancelWait.md) (src/backend/replication/syncrep.c:410)
  - [SyncRepCleanupAtProcExit](../S/SyncRepCleanupAtProcExit.md) (src/backend/replication/syncrep.c:428)
  - [SyncRepWakeQueue](../S/SyncRepWakeQueue.md) (src/backend/replication/syncrep.c:930)
  - SummarizeOldestCommittedSxact (src/backend/storage/lmgr/predicate.c:1521)
  - [ClearOldPredicateLocks](../C/ClearOldPredicateLocks.md) (src/backend/storage/lmgr/predicate.c:3711, 3728)
  - [dlist_delete_from_thoroughly](dlist_delete_from_thoroughly.md) (src/include/lib/ilist.h:443)

## Notes and Other Information
- This is an inline function for performance optimization
- Unlike dlist_delete, this function modifies the deleted node's pointers to NULL for safety
- The NULL pointers serve as a clear indicator that the node is not currently in any list
- Helps prevent bugs related to double deletion or use of stale list pointers
- Particularly useful in scenarios where node lifetime management is complex
- Used primarily in synchronous replication and serializable transaction isolation systems where precise cleanup is critical
- Provides better debugging capabilities as NULL pointers are easier to detect than stale pointers
- The additional pointer clearing operations have minimal performance overhead while providing significant safety benefits