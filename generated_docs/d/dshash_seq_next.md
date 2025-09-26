# dshash_seq_next

## Location
src/backend/lib/dshash.c: 657 - 746

## Overview
Returns the next element during sequential scanning of a dynamic shared hash table, managing partition locks and bucket traversal.

## Definition
```c
void *dshash_seq_next(dshash_seq_status *status)
```

## Detailed Description
dshash_seq_next implements the core iteration logic for sequential scanning through a dynamic shared hash table. The function traverses buckets in order, managing partition locks appropriately to ensure thread safety while allowing concurrent access. It handles partition transitions by acquiring locks in the correct order to avoid deadlocks, and maintains scan state to support element deletion during iteration. The function returns a pointer to the next element's data or NULL when the scan is complete.

## Parameters / Member Variables
- `status`: Pointer to the scan status structure that tracks the current position and state of the iteration

## Dependencies
- Functions called/Symbols referenced:
  - dsa_pointer (DSA pointer type)
  - ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME (assertion macro)
  - PARTITION_LOCK (partition lock macro)
  - LW_SHARED/LW_EXCLUSIVE (lock mode constants)
  - ensure_valid_bucket_pointers (bucket validation function)
  - NUM_BUCKETS (bucket count macro)
  - LWLockHeldByMeInMode (lock check function)
  - DsaPointerIsValid (DSA pointer validation)
  - PARTITION_FOR_BUCKET_INDEX (partition mapping macro)
  - dsa_get_address (DSA address resolution)
  - ENTRY_FROM_ITEM (item-to-entry conversion macro)
- Called from (representative examples):
  - pgstat_build_snapshot (src/backend/utils/activity/pgstat.c:1000)
  - pgstat_write_statsfile (src/backend/utils/activity/pgstat.c:1391)
  - pgstat_drop_database_and_contents (src/backend/utils/activity/pgstat_shmem.c:888)
  - pgstat_drop_all_entries (src/backend/utils/activity/pgstat_shmem.c:978)
  - pgstat_reset_matching_entries (src/backend/utils/activity/pgstat_shmem.c:1037)

## Notes and Other Information
- The function maintains partition locks throughout the scan to prevent resize operations
- Lock acquisition order is carefully managed to avoid deadlocks during partition transitions
- Supports both shared and exclusive scanning modes based on the initial dshash_seq_init() call
- Returned elements remain locked until the next call to dshash_seq_next() or dshash_seq_term()
- The function stores the next item pointer to support safe deletion of the current item via dshash_delete_current()
- Returns NULL when all elements have been processed, indicating the end of the scan
- Primarily used in PostgreSQL's statistics system for iterating through shared statistics data