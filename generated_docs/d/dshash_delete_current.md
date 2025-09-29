# dshash_delete_current

## Location
[src/backend/lib/dshash.c:757-777](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L757-L777)

## Overview
Safely removes the current entry during an exclusive sequential scan of a dynamic shared hash table.

## Definition
```c
void dshash_delete_current(dshash_seq_status *status)
```

## Detailed Description
dshash_delete_current provides the ability to safely delete the current item during a sequential scan of a dynamic shared hash table. This function can only be used when the scan was initiated in exclusive mode (exclusive=true in dshash_seq_init). It performs several safety checks to ensure the operation is valid, including verifying that exclusive locks are held and that the hash table state is consistent. The actual deletion is delegated to the delete_item() function.

## Parameters / Member Variables
- `status`: Pointer to the scan status structure that contains the current item to be deleted and maintains scan state

## Dependencies
- Functions called/Symbols referenced:
  - [dshash_seq_status](dshash_seq_status.md) (scan status structure type)
  - [dshash_table](dshash_table.md) (hash table structure type)
  - [dshash_table_item](dshash_table_item.md) (hash table item structure type)
  - PG_USED_FOR_ASSERTS_ONLY (assertion-only variable macro)
  - PARTITION_FOR_HASH (hash-to-partition mapping macro)
  - DSHASH_MAGIC (hash table magic number constant)
  - PARTITION_LOCK (partition lock macro)
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md) (lock verification function)
  - [delete_item](delete_item.md) (actual deletion implementation function)
- Called from (representative examples):
  - [pgstat_free_entry](../p/pgstat_free_entry.md) (src/backend/utils/activity/pgstat_shmem.c:814)

## Notes and Other Information
- Can only be used during exclusive scans (scan must be initialized with exclusive=true)
- Must be called only when a current item exists (after successful dshash_seq_next() call)
- Requires that the appropriate partition lock is held in exclusive mode
- Includes multiple assertions to verify safe deletion conditions
- The function validates the hash table magic number to ensure data structure integrity
- Primarily used in PostgreSQL's statistics system for removing obsolete or unwanted statistics entries
- After deletion, the scan can continue normally with subsequent dshash_seq_next() calls
- The deleted item's memory is properly freed and returned to the dynamic shared area

## Simplified Source

```c
void
dshash_delete_current(dshash_seq_status *status)
{
    dshash_table *hash_table = status->hash_table;
    dshash_table_item *item = status->curitem;

    // Verify scan is in exclusive mode and locks are held
    Assert(status->exclusive);
    Assert(hash_table->control->magic == DSHASH_MAGIC);

    // Delete the current item
    delete_item(hash_table, item);
}
```