# slist_push_head

## Location
[src/include/lib/ilist.h:1006-1017](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L1006-L1017)

## Overview
Inserts a node at the beginning of a singly linked list in PostgreSQL's intrusive list implementation.

## Definition

```c
static inline void
slist_push_head(slist_head *head, slist_node *node)
```
## Detailed Description
This function implements the standard "push to head" operation for PostgreSQL's singly linked list data structure. It efficiently adds a new node to the front of the list by updating the node's next pointer to point to the current first element, then updating the head's next pointer to point to the new node. The operation runs in O(1) constant time and is implemented as an inline function for optimal performance.

The function maintains the integrity of the list structure and includes a debug check to validate the list state after the insertion. This is part of PostgreSQL's intrusive list implementation where list nodes are embedded within the actual data structures rather than being separate allocations.

## Parameters / Member Variables
- `*head`: Pointer to the list head structure that maintains the list state
- `*node`: Pointer to the node to be inserted at the beginning of the list
## Dependencies
- Functions called/Symbols referenced:
  - [slist_check](slist_check.md) (for list integrity validation)
- Data types used:
  - [slist_head](slist_head.md)
  - [slist_node](slist_node.md)
- Called from (representative examples):
  - [EventTriggerSQLDropAddObject](../E/EventTriggerSQLDropAddObject.md) (src/backend/commands/event_trigger.c:1385)
  - [spi_dest_startup](spi_dest_startup.md) (src/backend/executor/spi.c:2154)
  - [BackgroundWorkerStateChange](../B/BackgroundWorkerStateChange.md) (src/backend/postmaster/bgworker.c:416)
  - [RegisterBackgroundWorker](../R/RegisterBackgroundWorker.md) (src/backend/postmaster/bgworker.c:956)
  - [on_dsm_detach](../o/on_dsm_detach.md) (src/backend/storage/ipc/dsm.c:1140)
  - [InitCatCache](../I/InitCatCache.md) (src/backend/utils/cache/catcache.c:971)
  - [ResetAllOptions](../R/ResetAllOptions.md) (src/backend/utils/misc/guc.c:2100)
  - [push_old_value](../p/push_old_value.md) (src/backend/utils/misc/guc.c:2208)
  - [AtEOXact_GUC](../A/AtEOXact_GUC.md) (src/backend/utils/misc/guc.c:2533)

## Notes and Other Information
- This is an inline function for maximum performance in list operations
- The function assumes the node being inserted is not already part of another list
- [List](../L/List.md) integrity is validated through slist_check() in debug builds
- Part of PostgreSQL's efficient intrusive list implementation that avoids separate memory allocations for list nodes
- The insertion operation is atomic and does not require any special synchronization for single-threaded use

## Simplified Source

```c
// Simplified version of slist_push_head
static inline void slist_push_head(slist_head *head, slist_node *node) {
    // Step 1: Point new node to current first element
    node->next = head->head.next;

    // Step 2: Make head point to new node (making it the new first element)
    head->head.next = node;

    // Step 3: Validate list integrity (debug builds only)
    slist_check(head);
}
```

Key simplifications made:
- Added clear step-by-step comments explaining the insertion logic
- Emphasized the two-step pointer manipulation that implements the insertion
- Maintained the original function structure as it's already quite clean
- Preserved the debug validation call for completeness