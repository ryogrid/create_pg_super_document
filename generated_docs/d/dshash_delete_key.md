# dshash_delete_key

## Location
[src/backend/lib/dshash.c:503-540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L503-L540)

## Overview
The  function removes an entry from a dynamic shared hash table based on a provided key and returns whether the deletion was successful.

## Definition

```c
bool
dshash_delete_key(dshash_table *hash_table, const void *key)
```
## Detailed Description
The  function performs a key-based deletion operation on a dynamic shared hash table. It first computes the hash value for the provided key and determines the appropriate partition. After acquiring an exclusive lock on the partition, it searches for and attempts to delete the entry matching the key. If the entry is found and successfully deleted, the function decrements the partition's entry count and returns true. If no matching entry is found, it returns false. The function ensures thread safety through exclusive locking and maintains accurate count tracking for load factor calculations.

## Parameters / Member Variables
- : Pointer to the dynamic shared hash table to operate on
- : Pointer to the key of the entry to be deleted

## Dependencies
- Functions called/Symbols referenced:
  - [hash_key](../h/hash_key.md): Computes hash value for the given key
  - PARTITION_FOR_HASH: Macro to determine partition from hash value
  - PARTITION_LOCK: Macro to get partition lock
  - ensure_valid_bucket_pointers: Ensures bucket pointers are valid
  - [delete_key_from_bucket](delete_key_from_bucket.md): Performs actual deletion from the bucket
  - BUCKET_FOR_HASH: Macro to determine bucket from hash value
  - LWLockAcquire: Acquires exclusive lightweight lock
  - LWLockRelease: Releases lightweight lock
- Called from (representative examples):
  - ApplyLauncherForgetWorkerStartTime: Removing worker startup time records
  - [find_or_make_matching_shared_tupledesc](../f/find_or_make_matching_shared_tupledesc.md): Type descriptor cleanup operations

## Notes and Other Information
- Always uses exclusive locking to ensure atomicity of the deletion operation
- Returns a boolean to indicate success or failure of the deletion
- Automatically decrements the partition count when an entry is successfully deleted
- Does not require the caller to have a direct pointer to the entry being deleted
- For cases where the caller already has an entry pointer,  is more efficient
- The function includes assertions to ensure the hash table is in a valid state
- No lock is held after the function returns, unlike find operations