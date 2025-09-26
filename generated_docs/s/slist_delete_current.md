# slist_delete_current

## Location
[src/include/lib/ilist.h:1084-1105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L1084-L1105)

## Overview
Deletes the list element that a mutable iterator currently points to during singly-linked list traversal.

## Definition
```c
static inline void
slist_delete_current(slist_mutable_iter *iter)
```

## Detailed Description
This function removes the current element from a singly-linked list during iteration using a mutable iterator. It updates the previous element's forward link to skip over the current element, effectively removing it from the list. The function also resets the iterator's current pointer to the previous element to maintain proper iteration state. This allows safe deletion of elements during list traversal without breaking the iteration sequence.

## Parameters / Member Variables
- `iter`: Pointer to a mutable iterator structure used for list traversal that supports element deletion

## Dependencies
- Functions called/Symbols referenced:
  - [slist_mutable_iter](slist_mutable_iter.md) (structure type)
- Called from (representative examples):
  - [AtEOSubXact_SPI](../A/AtEOSubXact_SPI.md) (in src/backend/executor/spi.c:566)
  - [SPI_freetuptable](../S/SPI_freetuptable.md) (in src/backend/executor/spi.c:1409)
  - [ForgetBackgroundWorker](../F/ForgetBackgroundWorker.md) (in src/backend/postmaster/bgworker.c:457)
  - [cancel_on_dsm_detach](../c/cancel_on_dsm_detach.md) (in src/backend/storage/ipc/dsm.c:1159)
  - [AtEOXact_GUC](../A/AtEOXact_GUC.md) (in src/backend/utils/misc/guc.c:2525)
  - [ReportChangedGUCOptions](../R/ReportChangedGUCOptions.md) (in src/backend/utils/misc/guc.c:2625)

## Notes and Other Information
- This is a static inline function for performance optimization
- Modifies iter->cur, so the current iterator position should not be used again in the same loop iteration
- Designed to work with slist_foreach_modify() macro for safe iteration with deletion
- Handles deletion of the first element correctly by using the list header's "head" field as the previous pointer
- Part of PostgreSQL's intrusive linked list implementation in src/include/lib/ilist.h
- Used throughout PostgreSQL for cleanup operations during transaction end and resource management