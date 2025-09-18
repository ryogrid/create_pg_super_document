# slist_push_head

## Location
src/include/lib/ilist.h: 1006 - 1017

## Overview
Inserts a node at the beginning of a singly linked list in PostgreSQL's intrusive list implementation.

## Definition


## Detailed Description
This function implements the standard "push to head" operation for PostgreSQL's singly linked list data structure. It efficiently adds a new node to the front of the list by updating the node's next pointer to point to the current first element, then updating the head's next pointer to point to the new node. The operation runs in O(1) constant time and is implemented as an inline function for optimal performance.

The function maintains the integrity of the list structure and includes a debug check to validate the list state after the insertion. This is part of PostgreSQL's intrusive list implementation where list nodes are embedded within the actual data structures rather than being separate allocations.

## Parameters / Member Variables
- : Pointer to the list head structure that maintains the list state
- : Pointer to the node to be inserted at the beginning of the list

## Dependencies
- Functions called/Symbols referenced:
  - slist_check (for list integrity validation)
- Data types used:
  - slist_head
  - slist_node
- Called from (representative examples):
  - EventTriggerSQLDropAddObject (src/backend/commands/event_trigger.c:1385)
  - spi_dest_startup (src/backend/executor/spi.c:2154)
  - BackgroundWorkerStateChange (src/backend/postmaster/bgworker.c:416)
  - RegisterBackgroundWorker (src/backend/postmaster/bgworker.c:956)
  - on_dsm_detach (src/backend/storage/ipc/dsm.c:1140)
  - InitCatCache (src/backend/utils/cache/catcache.c:971)
  - ResetAllOptions (src/backend/utils/misc/guc.c:2100)
  - push_old_value (src/backend/utils/misc/guc.c:2208)
  - AtEOXact_GUC (src/backend/utils/misc/guc.c:2533)

## Notes and Other Information
- This is an inline function for maximum performance in list operations
- The function assumes the node being inserted is not already part of another list
- List integrity is validated through slist_check() in debug builds
- Part of PostgreSQL's efficient intrusive list implementation that avoids separate memory allocations for list nodes
- The insertion operation is atomic and does not require any special synchronization for single-threaded use