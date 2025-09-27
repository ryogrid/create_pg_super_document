# dshash_seq_next

## Location
[src/backend/lib/dshash.c:657-746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L657-L746)

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
  - [ensure_valid_bucket_pointers](../e/ensure_valid_bucket_pointers.md) (bucket validation function)
  - NUM_BUCKETS (bucket count macro)
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md) (lock check function)
  - DsaPointerIsValid (DSA pointer validation)
  - PARTITION_FOR_BUCKET_INDEX (partition mapping macro)
  - [dsa_get_address](dsa_get_address.md) (DSA address resolution)
  - ENTRY_FROM_ITEM (item-to-entry conversion macro)
- Called from (representative examples):
  - [pgstat_build_snapshot](../p/pgstat_build_snapshot.md) (src/backend/utils/activity/pgstat.c:1000)
  - [pgstat_write_statsfile](../p/pgstat_write_statsfile.md) (src/backend/utils/activity/pgstat.c:1391)
  - [pgstat_drop_database_and_contents](../p/pgstat_drop_database_and_contents.md) (src/backend/utils/activity/pgstat_shmem.c:888)
  - [pgstat_drop_all_entries](../p/pgstat_drop_all_entries.md) (src/backend/utils/activity/pgstat_shmem.c:978)
  - [pgstat_reset_matching_entries](../p/pgstat_reset_matching_entries.md) (src/backend/utils/activity/pgstat_shmem.c:1037)

## Notes and Other Information
- The function maintains partition locks throughout the scan to prevent resize operations
- Lock acquisition order is carefully managed to avoid deadlocks during partition transitions
- Supports both shared and exclusive scanning modes based on the initial dshash_seq_init() call
- Returned elements remain locked until the next call to dshash_seq_next() or dshash_seq_term()
- The function stores the next item pointer to support safe deletion of the current item via dshash_delete_current()
- Returns NULL when all elements have been processed, indicating the end of the scan
- Primarily used in PostgreSQL's statistics system for iterating through shared statistics data

## Simplified Source

```c
// Simplified version of dshash_seq_next
void *dshash_seq_next(dshash_seq_status *status) {
    dsa_pointer next_item_pointer;

    // Initialize scan: lock first partition and setup bucket pointers
    if (status->curpartition == -1) {
        status->curpartition = 0;
        LWLockAcquire(PARTITION_LOCK(status->hash_table, 0),
                     status->exclusive ? LW_EXCLUSIVE : LW_SHARED);
        ensure_valid_bucket_pointers(status->hash_table);
        status->nbuckets = NUM_BUCKETS(status->hash_table->control->size_log2);
        next_item_pointer = status->hash_table->buckets[status->curbucket];
    } else {
        next_item_pointer = status->pnextitem;
    }

    // Skip empty buckets and handle partition transitions
    while (!DsaPointerIsValid(next_item_pointer)) {
        if (++status->curbucket >= status->nbuckets) {
            return NULL;  // Scan complete
        }

        // Check if we need to move to next partition
        int next_partition = PARTITION_FOR_BUCKET_INDEX(status->curbucket,
                                                       status->hash_table->size_log2);

        if (status->curpartition != next_partition) {
            // Lock next partition first, then release current (avoid deadlock)
            LWLockAcquire(PARTITION_LOCK(status->hash_table, next_partition),
                         status->exclusive ? LW_EXCLUSIVE : LW_SHARED);
            LWLockRelease(PARTITION_LOCK(status->hash_table, status->curpartition));
            status->curpartition = next_partition;
        }

        next_item_pointer = status->hash_table->buckets[status->curbucket];
    }

    // Get current item and prepare for next iteration
    status->curitem = dsa_get_address(status->hash_table->area, next_item_pointer);
    status->pnextitem = status->curitem->next;  // Store next for safe deletion

    return ENTRY_FROM_ITEM(status->curitem);
}
```

Key simplifications made:
- Removed detailed assertions and complex comments
- Consolidated partition transition logic
- Focused on the main iteration flow
- Simplified bucket traversal logic
- Maintained essential locking order for deadlock avoidance