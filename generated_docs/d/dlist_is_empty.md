# dlist_is_empty

## Location
[src/include/lib/ilist.h:336-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L336-L346)

## Overview
Checks whether a doubly-linked list is empty by examining the head node's next pointer for NULL or self-reference conditions.

## Definition


## Detailed Description
The  function determines if a doubly-linked list contains any elements by checking two possible empty states: either the head's next pointer is NULL (uninitialized state) or it points to itself (properly initialized empty list). This dual check accommodates both uninitialized lists and lists that have been properly initialized with . The function includes a  call for debugging builds to validate list integrity before performing the emptiness test. This design allows the function to work reliably regardless of whether the list was initialized or is in an uninitialized state.

## Parameters / Member Variables
- : Pointer to the  structure to be checked for emptiness (const-qualified for read-only access)

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_head](dlist_head.md) (structure type)
  - [dlist_check](dlist_check.md) (integrity validation function)
- Called from (representative examples):
  - [dataBeginPlaceToPageLeaf](dataBeginPlaceToPageLeaf.md) (src/backend/access/gin/gindatapage.c:499)
  - ParallelContextActive (src/backend/access/transam/parallel.c:1022)
  - [AtEOSubXact_Parallel](../A/AtEOSubXact_Parallel.md) (src/backend/access/transam/parallel.c:1252)
  - [launcher_determine_sleep](../l/launcher_determine_sleep.md) (src/backend/postmaster/autovacuum.c:805)
  - ReorderBufferGetOldestTXN (src/backend/replication/logical/reorderbuffer.c:1046)
  - [SetupLockInTable](../S/SetupLockInTable.md) (src/backend/storage/lmgr/lock.c:1253)
  - [CleanUpLock](../C/CleanUpLock.md) (src/backend/storage/lmgr/lock.c:1669)
  - InitProcess (src/backend/storage/lmgr/proc.c:336)

## Notes and Other Information
- The function is implemented as a static inline function for performance efficiency
- Handles both initialized (self-pointing) and uninitialized (NULL) empty list states
- Includes integrity checking via  in debug builds
- Widely used across PostgreSQL subsystems including GIN indexes, parallel processing, replication, locking, and memory management
- The const qualifier on the parameter ensures the function doesn't modify the list structure
- Located in src/include/lib/ilist.h:336-346