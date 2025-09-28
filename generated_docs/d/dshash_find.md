# dshash_find

## Location
[src/backend/lib/dshash.c:390-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L390-L432)

## Overview
The  function looks up an entry in a dynamic shared hash table given a key, returning either a pointer to the found entry or NULL if not found.

## Definition

```c
void *
dshash_find(dshash_table *hash_table, const void *key, bool exclusive)
```
## Detailed Description
The  function performs a hash table lookup operation in PostgreSQL's dynamic shared hash table implementation. It searches for an entry matching the provided key and returns a pointer to the entry if found. The function acquires an LWLock on the appropriate partition of the hash table during the search operation, with the lock mode determined by the  parameter. If an entry is found, the lock is maintained and must be explicitly released by the caller using . The function ensures thread safety in multi-process environments by using lightweight locks and includes assertions to prevent deadlock scenarios.

## Parameters / Member Variables
- : Pointer to the dynamic shared hash table to search in
- : Pointer to the key to search for in the hash table
- : Boolean flag determining lock mode - true for exclusive (LW_EXCLUSIVE), false for shared (LW_SHARED)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_key](../h/hash_key.md): Computes hash value for the given key
  - PARTITION_FOR_HASH: Macro to determine partition from hash value
  - PARTITION_LOCK: Macro to get partition lock
  - [ensure_valid_bucket_pointers](../e/ensure_valid_bucket_pointers.md): Ensures bucket pointers are valid
  - BUCKET_FOR_HASH: Macro to determine bucket from hash value
  - [find_in_bucket](../f/find_in_bucket.md): Searches for item within a specific bucket
  - ENTRY_FROM_ITEM: Macro to convert item to entry pointer
  - [LWLockAcquire](../L/LWLockAcquire.md): Acquires lightweight lock
  - [LWLockRelease](../L/LWLockRelease.md): Releases lightweight lock
- Called from (representative examples):
  - [ApplyLauncherGetWorkerStartTime](../A/ApplyLauncherGetWorkerStartTime.md): Worker startup time retrieval
  - [pgstat_get_entry_ref](../p/pgstat_get_entry_ref.md): Statistics entry reference retrieval
  - [lookup_rowtype_tupdesc_internal](../l/lookup_rowtype_tupdesc_internal.md): Type cache lookup operations

## Notes and Other Information
- The function holds interrupts while the lock is active, which are only resumed when  is called
- Callers must not already hold a partition lock when calling this function
- The returned entry pointer remains valid only while the lock is held
- If an error occurs before releasing the lock, the lock is automatically released but the caller must ensure entry integrity
- The function includes magic number validation to ensure hash table integrity

## Simplified Source

```c
// Simplified version of dshash_find
void *dshash_find(dshash_table *hash_table, const void *key, bool exclusive) {
    dshash_hash hash;
    size_t partition;
    dshash_table_item *item;

    hash = hash_key(hash_table, key);
    partition = PARTITION_FOR_HASH(hash);

    Assert(hash_table->control->magic == DSHASH_MAGIC);
    ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME(hash_table);

    // Acquire partition lock (shared or exclusive)
    LWLockAcquire(PARTITION_LOCK(hash_table, partition),
                  exclusive ? LW_EXCLUSIVE : LW_SHARED);
    ensure_valid_bucket_pointers(hash_table);

    // Search the bucket for the key
    item = find_in_bucket(hash_table, key, BUCKET_FOR_HASH(hash_table, hash));

    if (!item) {
        // Not found - release lock and return NULL
        LWLockRelease(PARTITION_LOCK(hash_table, partition));
        return NULL;
    } else {
        // Found - return entry (caller must release lock)
        return ENTRY_FROM_ITEM(item);
    }
}
```

Key simplifications made:
- Consolidated lock acquisition with clear mode selection
- Simplified the found/not-found logic flow
- Maintained critical assertions and bucket validation
- Preserved the caller-must-release-lock contract
- Focused on the core hash-and-search operation