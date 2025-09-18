# dshash_find

## Location
[src/backend/lib/dshash.c:390-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L390-L432)

## Overview
The  function looks up an entry in a dynamic shared hash table given a key, returning either a pointer to the found entry or NULL if not found.

## Definition


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
  - ensure_valid_bucket_pointers: Ensures bucket pointers are valid
  - BUCKET_FOR_HASH: Macro to determine bucket from hash value
  - find_in_bucket: Searches for item within a specific bucket
  - ENTRY_FROM_ITEM: Macro to convert item to entry pointer
  - LWLockAcquire: Acquires lightweight lock
  - LWLockRelease: Releases lightweight lock
- Called from (representative examples):
  - ApplyLauncherGetWorkerStartTime: Worker startup time retrieval
  - pgstat_get_entry_ref: Statistics entry reference retrieval
  - [lookup_rowtype_tupdesc_internal](../l/lookup_rowtype_tupdesc_internal.md): Type cache lookup operations

## Notes and Other Information
- The function holds interrupts while the lock is active, which are only resumed when  is called
- Callers must not already hold a partition lock when calling this function
- The returned entry pointer remains valid only while the lock is held
- If an error occurs before releasing the lock, the lock is automatically released but the caller must ensure entry integrity
- The function includes magic number validation to ensure hash table integrity