# dlist_push_head

## Location
[src/include/lib/ilist.h:347-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L347-L363)

## Overview
Inserts a new node at the beginning of a doubly-linked list, automatically handling both initialized and uninitialized list states.

## Definition

```c
static inline void
dlist_push_head(dlist_head *head, dlist_node *node)
```
## Detailed Description
The  function adds a new node to the front of a doubly-linked list by updating the necessary pointer relationships. It intelligently handles uninitialized lists by checking if the head's next pointer is NULL and automatically calling  to convert it to a proper circular structure. The function then inserts the new node between the head and the current first element, updating all four relevant pointers: the new node's next and prev pointers, the old first element's prev pointer, and the head's next pointer. After insertion, it calls  to validate list integrity in debug builds.

## Parameters / Member Variables
- `*head`: Pointer to the  structure representing the list to insert into
- `*node`: Pointer to the  structure to be inserted at the beginning of the list
## Dependencies
- Functions called/Symbols referenced:
  - [dlist_head](dlist_head.md) (structure type)
  - [dlist_node](dlist_node.md) (structure type)
  - [dlist_init](dlist_init.md) (initialization function)
  - [dlist_check](dlist_check.md) (integrity validation function)
- Called from (representative examples):
  - [CreateParallelContext](../C/CreateParallelContext.md) (src/backend/access/transam/parallel.c:193)
  - [XLogPrefetcherAddFilter](../X/XLogPrefetcherAddFilter.md) (src/backend/access/transam/xlogprefetcher.c:872)
  - [rebuild_database_list](../r/rebuild_database_list.md) (src/backend/postmaster/autovacuum.c:1041)
  - [AutoVacWorkerMain](../A/AutoVacWorkerMain.md) (src/backend/postmaster/autovacuum.c:1513)
  - [BackendStartup](../B/BackendStartup.md) (src/backend/postmaster/postmaster.c:3623)
  - [SyncRepQueueInsert](../S/SyncRepQueueInsert.md) (src/backend/replication/syncrep.c:399)
  - [BecomeLockGroupLeader](../B/BecomeLockGroupLeader.md) (src/backend/storage/lmgr/proc.c:1913)

## Notes and Other Information
- The function is implemented as a static inline function for performance efficiency
- Automatically initializes uninitialized lists (NULL head) by calling 
- Maintains proper doubly-linked list invariants by updating all necessary pointer relationships
- Includes integrity checking via  in debug builds
- Commonly used in PostgreSQL's parallel processing, autovacuum, replication, and memory management subsystems
- The node being inserted should not already be part of another list to avoid corruption
- Located in src/include/lib/ilist.h:347-363

## Simplified Source

```c
// Simplified version of dlist_push_head
static inline void
dlist_push_head(dlist_head *head, dlist_node *node)
{
    // Initialize empty list if needed
    if (head->head.next == NULL)
        dlist_init(head);

    // Connect new node to current first element
    node->next = head->head.next;
    node->prev = &head->head;

    // Update pointers to insert node at head
    node->next->prev = node;
    head->head.next = node;

    // Validate list integrity (debug builds only)
    dlist_check(head);
}
```

Key simplifications made:
- Added clear step-by-step comments explaining the insertion process
- Grouped related pointer updates logically
- Emphasized the automatic list initialization feature
- Focused on the core doubly-linked list insertion algorithm
- Maintained all essential functionality while improving readability