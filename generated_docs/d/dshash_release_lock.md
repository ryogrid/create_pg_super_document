# dshash_release_lock

## Location
[src/backend/lib/dshash.c:558-571](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L558-L571)

## Overview
The  function releases the lock on an entry that was previously acquired through  or .

## Definition

```c
void
dshash_release_lock(dshash_table *hash_table, void *entry)
```
## Detailed Description
The  function is responsible for releasing the lightweight lock (LWLock) that was acquired when an entry was found or inserted through  or . The function determines the appropriate partition by extracting the hash value stored in the entry's associated item structure, then releases the corresponding partition lock. This function is essential for proper resource management and preventing deadlocks in the dynamic shared hash table system. After calling this function, the entry pointer should not be accessed as it may be modified by other processes.

## Parameters / Member Variables
- : Pointer to the dynamic shared hash table containing the entry
- : Pointer to the entry whose lock should be released (must have been obtained through dshash_find or dshash_find_or_insert)

## Dependencies
- Functions called/Symbols referenced:
  - ITEM_FROM_ENTRY: Macro to convert entry pointer to internal item structure
  - PARTITION_FOR_HASH: Macro to determine partition index from hash value
  - PARTITION_LOCK: Macro to get the appropriate partition lock
  - [LWLockRelease](../L/LWLockRelease.md): Releases the lightweight lock on the partition
- Called from (representative examples):
  - [ApplyLauncherSetWorkerStartTime](../A/ApplyLauncherSetWorkerStartTime.md): After setting worker startup times
  - [ApplyLauncherGetWorkerStartTime](../A/ApplyLauncherGetWorkerStartTime.md): After retrieving worker startup times
  - [GetNamedDSMSegment](../G/GetNamedDSMSegment.md): After DSM segment operations
  - [pgstat_get_entry_ref](../p/pgstat_get_entry_ref.md): After statistics entry reference operations
  - [lookup_rowtype_tupdesc_internal](../l/lookup_rowtype_tupdesc_internal.md): After type descriptor lookups
  - [find_or_make_matching_shared_tupledesc](../f/find_or_make_matching_shared_tupledesc.md): After tuple descriptor operations

## Notes and Other Information
- Must only be called on entries obtained through  or 
- Resumes interrupts that were held during the locked period
- The entry pointer becomes potentially unsafe to access after this function returns
- Does not perform any validation that the caller actually holds the lock being released
- Essential for preventing resource leaks and deadlocks in multi-process environments
- Should be called as quickly as possible to minimize lock contention
- Used extensively throughout PostgreSQL's statistics and caching subsystems