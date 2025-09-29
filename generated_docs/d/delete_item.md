# delete_item

## Location
[src/backend/lib/dshash.c:832-857](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L832-L857)

## Overview
A static helper function that removes a locked item from a dynamic shared hash table and updates the corresponding partition count.

## Definition
```c
static void delete_item(dshash_table *hash_table, dshash_table_item *item)
```

## Detailed Description
The `delete_item` function performs the removal of a specific hash table item that is already locked. It operates by first determining which partition the item belongs to based on its hash value, then delegating the actual bucket manipulation to `delete_item_from_bucket`. After successful removal from the bucket chain, it decrements the item count for the corresponding partition.

This function is part of the internal implementation of the dynamic shared hash table deletion operations and requires that the caller has already acquired the appropriate partition lock. The function includes assertions to ensure proper locking discipline and to verify that the deletion operation succeeds as expected.

## Parameters / Member Variables
- `hash_table`: Pointer to the dshash_table structure representing the dynamic shared hash table
- `item`: Pointer to the dshash_table_item to be deleted from the hash table

## Dependencies
- Functions called/Symbols referenced:
  - PARTITION_FOR_HASH (macro to determine partition from hash)
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md) (assertion check for lock ownership)
  - PARTITION_LOCK (macro to get partition lock)
  - [delete_item_from_bucket](delete_item_from_bucket.md) (performs actual bucket removal)
  - BUCKET_FOR_HASH (macro to locate the bucket for the hash)
  - Assert (debugging assertions)
- Types used:
  - [dshash_table](dshash_table.md)
  - [dshash_table_item](dshash_table_item.md)
- Called from (representative examples):
  - [dshash_delete_entry](dshash_delete_entry.md)
  - [dshash_delete_current](dshash_delete_current.md)
  - [SH_DELETE_ITEM](../S/SH_DELETE_ITEM.md) (from simplehash.h)

## Notes and Other Information
- This is a static function, only accessible within the dshash.c compilation unit
- The caller must hold the partition lock for the item being deleted
- The function assumes the item pointer is valid and properly initialized
- Includes an assertion that should never fail - if delete_item_from_bucket returns false, it indicates a serious internal error
- The partition count is automatically decremented upon successful deletion
- Part of the low-level hash table management infrastructure

## Simplified Source

```c
static void delete_item(dshash_table *hash_table, dshash_table_item *item) {
    size_t hash = item->hash;
    size_t partition = PARTITION_FOR_HASH(hash);

    // Verify we hold the partition lock
    Assert(LWLockHeldByMe(PARTITION_LOCK(hash_table, partition)));

    // Remove item from its bucket
    if (delete_item_from_bucket(hash_table, item,
                                &BUCKET_FOR_HASH(hash_table, hash))) {
        // Decrement partition count on successful deletion
        Assert(hash_table->control->partitions[partition].count > 0);
        --hash_table->control->partitions[partition].count;
    } else {
        // This should never happen
        Assert(false);
    }
}
```