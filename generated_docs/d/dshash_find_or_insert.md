# dshash_find_or_insert

## Location
[src/backend/lib/dshash.c:433-502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L433-L502)

## Overview
The  function searches for an entry in a dynamic shared hash table and either returns the existing entry or creates and returns a new one if not found.

## Definition


## Detailed Description
The  function performs an atomic find-or-insert operation on a dynamic shared hash table. It first attempts to locate an existing entry with the provided key. If found, it returns a pointer to the existing entry and sets the  parameter to true. If not found, it creates a new entry, inserts it into the hash table, and returns a pointer to the newly created entry while setting  to false. The function maintains an exclusive lock throughout the operation and includes automatic hash table resizing when the load factor exceeds 0.75. The resize operation requires releasing and reacquiring locks in the proper order to prevent deadlocks.

## Parameters / Member Variables
- : Pointer to the dynamic shared hash table to operate on
- : Pointer to the key to search for or insert
- : Pointer to boolean flag that will be set to true if entry was found, false if newly created

## Dependencies
- Functions called/Symbols referenced:
  - [hash_key](../h/hash_key.md): Computes hash value for the given key
  - PARTITION_FOR_HASH: Macro to determine partition from hash value
  - PARTITION_LOCK: Macro to get partition lock
  - ensure_valid_bucket_pointers: Ensures bucket pointers are valid
  - find_in_bucket: Searches for item within a specific bucket
  - BUCKET_FOR_HASH: Macro to determine bucket from hash value
  - MAX_COUNT_PER_PARTITION: Macro to determine maximum count per partition
  - resize: Resizes the hash table when load factor is too high
  - [insert_into_bucket](../i/insert_into_bucket.md): Inserts new item into specified bucket
  - ENTRY_FROM_ITEM: Macro to convert item to entry pointer
  - LWLockAcquire: Acquires exclusive lightweight lock
  - LWLockRelease: Releases lightweight lock
- Called from (representative examples):
  - ApplyLauncherSetWorkerStartTime: Setting worker startup times
  - GetNamedDSMSegment: DSM segment retrieval operations
  - pgstat_get_entry_ref: Statistics entry reference operations
  - [find_or_make_matching_shared_tupledesc](../f/find_or_make_matching_shared_tupledesc.md): Type descriptor matching operations

## Notes and Other Information
- Always acquires an exclusive lock, unlike  which can use shared locks
- Automatically triggers hash table resize when load factor exceeds 0.75 per partition
- The resize operation uses a restart mechanism with proper lock ordering to avoid deadlocks
- Load factor is tracked per partition rather than globally to avoid contention
- The returned entry pointer must be released using 
- Includes the same locking and error handling semantics as 