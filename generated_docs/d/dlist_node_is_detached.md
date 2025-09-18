# dlist_node_is_detached

## Location
src/include/lib/ilist.h: 525 - 536

## Overview
Checks whether a doubly-linked list node is currently detached (not part of any list).

## Definition


## Detailed Description
This function determines if a node is detached from any doubly-linked list by checking if both its next and prev pointers are NULL. A node is considered detached if it has been either:
1. Initialized with dlist_init_node() but never added to a list
2. Removed from a list using thorough deletion functions (dlist_delete_thoroughly, dlist_delete_from_thoroughly, or dclist_delete_from_thoroughly)

The function includes an assertion that enforces the invariant that both pointers should be either NULL (detached) or non-NULL (attached). This helps catch programming errors where a node might be in an inconsistent state.

The function is implemented as a static inline function for performance, as it's a simple pointer check that benefits from inlining and is likely to be called frequently in list management operations.

## Parameters / Member Variables
- : Pointer to the doubly-linked list node to check for detachment status

## Dependencies
- Functions called/Symbols referenced:
  - dlist_node (struct type)
  - Assert (macro for debug assertions)
- Called from (representative examples):
  - SyncRepWaitForLSN (src/backend/replication/syncrep.c:189, 356)
  - SyncRepCancelWait (src/backend/replication/syncrep.c:409)
  - SyncRepCleanupAtProcExit (src/backend/replication/syncrep.c:422, 427)
  - SxactIsOnFinishedList (src/backend/storage/lmgr/predicate.c:267)
  - LockErrorCleanup (src/backend/storage/lmgr/proc.c:769)
  - ProcWakeup (src/backend/storage/lmgr/proc.c:1685)

## Notes and Other Information
- The function enforces a strict invariant: both next and prev pointers must be either both NULL or both non-NULL
- Used extensively in PostgreSQL's process management and synchronization code
- Only returns true for nodes that have been explicitly detached through proper initialization or thorough deletion
- The assertion helps catch bugs where nodes might be left in inconsistent states during list operations