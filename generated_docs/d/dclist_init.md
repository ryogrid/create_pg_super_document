# dclist_init

## Location
src/include/lib/ilist.h: 671 - 681

## Overview
Initializes a doubly-linked count list (dclist) by setting up the underlying doubly-linked list and resetting the element count to zero.

## Definition
```c
static inline void
dclist_init(dclist_head *head)
```

## Detailed Description
This function initializes a doubly-linked count list data structure. A dclist is an extension of PostgreSQL's standard doubly-linked list (dlist) that additionally maintains a count of elements in the list. The function performs two operations: it calls `dlist_init` to initialize the underlying doubly-linked list structure, and it sets the count field to zero. Any previous state of the list is discarded without cleanup, so this should only be called on uninitialized lists or when the caller has already handled cleanup of existing elements.

## Parameters / Member Variables
- `head`: Pointer to the dclist_head structure to be initialized

## Dependencies
- Functions called/Symbols referenced:
  - dlist_init (initializes the underlying doubly-linked list)
  - dclist_head (parameter type)
- Called from (representative examples):
  - logical_rewrite_log_mapping (src/backend/access/heap/rewriteheap.c:969)
  - AtEOXact_MultiXact (src/backend/access/transam/multixact.c:1817)
  - PostPrepare_MultiXact (src/backend/access/transam/multixact.c:1883)
  - ReorderBufferAllocate (src/backend/replication/logical/reorderbuffer.c:397)
  - DeadLockCheck (src/backend/storage/lmgr/deadlock.c:260)
  - SetupLockInTable (src/backend/storage/lmgr/lock.c:1211)
  - dclist_push_head (src/include/lib/ilist.h:696)
  - dclist_push_tail (src/include/lib/ilist.h:712)

## Notes and Other Information
- Part of PostgreSQL's doubly-linked count list implementation in src/include/lib/ilist.h
- The function is static inline for performance optimization
- Previous state is discarded without cleanup - caller must handle any necessary cleanup
- dclist provides O(1) count operations compared to standard dlist which requires O(n) traversal
- Widely used throughout PostgreSQL for managing collections where element count is frequently needed