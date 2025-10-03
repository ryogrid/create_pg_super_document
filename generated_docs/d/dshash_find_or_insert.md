# dshash_find_or_insert

## Location
[src/backend/lib/dshash.c:433-502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L433-L502)

## Overview
The  function searches for an entry in a dynamic shared hash table and either returns the existing entry or creates and returns a new one if not found.

## Definition

```c
void *
dshash_find_or_insert(dshash_table *hash_table,
					  const void *key,
					  bool *found)
```
## Detailed Description
The  function performs an atomic find-or-insert operation on a dynamic shared hash table. It first attempts to locate an existing entry with the provided key. If found, it returns a pointer to the existing entry and sets the  parameter to true. If not found, it creates a new entry, inserts it into the hash table, and returns a pointer to the newly created entry while setting  to false. The function maintains an exclusive lock throughout the operation and includes automatic hash table resizing when the load factor exceeds 0.75. The resize operation requires releasing and reacquiring locks in the proper order to prevent deadlocks.

## Parameters / Member Variables
- `*hash_table`: Pointer to the dynamic shared hash table to operate on
- `*key`: Pointer to the key to search for or insert
- `*found`: Pointer to boolean flag that will be set to true if entry was found, false if newly created
## Dependencies
- Functions called/Symbols referenced:
  - [hash_key](../h/hash_key.md): Computes hash value for the given key
  - PARTITION_FOR_HASH: Macro to determine partition from hash value
  - PARTITION_LOCK: Macro to get partition lock
  - [ensure_valid_bucket_pointers](../e/ensure_valid_bucket_pointers.md): Ensures bucket pointers are valid
  - [find_in_bucket](../f/find_in_bucket.md): Searches for item within a specific bucket
  - BUCKET_FOR_HASH: Macro to determine bucket from hash value
  - MAX_COUNT_PER_PARTITION: Macro to determine maximum count per partition
  - [resize](../r/resize.md): Resizes the hash table when load factor is too high
  - [insert_into_bucket](../i/insert_into_bucket.md): Inserts new item into specified bucket
  - ENTRY_FROM_ITEM: Macro to convert item to entry pointer
  - [LWLockAcquire](../L/LWLockAcquire.md): Acquires exclusive lightweight lock
  - [LWLockRelease](../L/LWLockRelease.md): Releases lightweight lock
- Called from (representative examples):
  - [ApplyLauncherSetWorkerStartTime](../A/ApplyLauncherSetWorkerStartTime.md): Setting worker startup times
  - [GetNamedDSMSegment](../G/GetNamedDSMSegment.md): DSM segment retrieval operations
  - [pgstat_get_entry_ref](../p/pgstat_get_entry_ref.md): Statistics entry reference operations
  - [find_or_make_matching_shared_tupledesc](../f/find_or_make_matching_shared_tupledesc.md): Type descriptor matching operations

## Notes and Other Information
- Always acquires an exclusive lock, unlike  which can use shared locks
- Automatically triggers hash table resize when load factor exceeds 0.75 per partition
- The resize operation uses a restart mechanism with proper lock ordering to avoid deadlocks
- Load factor is tracked per partition rather than globally to avoid contention
- The returned entry pointer must be released using 
- Includes the same locking and error handling semantics as

## Simplified Source

```c
void *dshash_find_or_insert(dshash_table *hash_table, const void *key, bool *found)
{
    dshash_hash hash;
    size_t partition_index;
    dshash_partition *partition;
    dshash_table_item *item;

    // Compute hash and determine partition
    hash = hash_key(hash_table, key);
    partition_index = PARTITION_FOR_HASH(hash);
    partition = &hash_table->control->partitions[partition_index];

    Assert(hash_table->control->magic == DSHASH_MAGIC);
    ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME(hash_table);

restart:
    // Acquire exclusive lock on partition
    LWLockAcquire(PARTITION_LOCK(hash_table, partition_index), LW_EXCLUSIVE);
    ensure_valid_bucket_pointers(hash_table);

    // Search for existing item in bucket
    item = find_in_bucket(hash_table, key, BUCKET_FOR_HASH(hash_table, hash));

    if (item)
    {
        *found = true;
    }
    else
    {
        *found = false;

        // Check if hash table is getting too full (load factor > 0.75)
        if (partition->count > MAX_COUNT_PER_PARTITION(hash_table))
        {
            // Need to resize - release lock first to avoid deadlocks
            LWLockRelease(PARTITION_LOCK(hash_table, partition_index));
            resize(hash_table, hash_table->size_log2 + 1);
            goto restart;
        }

        // Insert new item into bucket
        item = insert_into_bucket(hash_table, key, &BUCKET_FOR_HASH(hash_table, hash));
        item->hash = hash;

        // Update partition counter for load factor tracking
        ++partition->count;
    }

    // Return entry pointer (caller must call dshash_release_lock)
    return ENTRY_FROM_ITEM(item);
}
``` 