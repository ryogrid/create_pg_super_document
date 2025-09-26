# dshash_delete_entry

## Location
[src/backend/lib/dshash.c:541-557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L541-L557)

## Overview
The  function removes an entry from a dynamic shared hash table using a direct entry pointer, automatically releasing the associated exclusive lock.

## Definition

```c
void
dshash_delete_entry(dshash_table *hash_table, void *entry)
```
## Detailed Description
The  function performs a direct deletion of an entry from a dynamic shared hash table when the caller already has a pointer to the entry. This function is more efficient than  when the entry pointer is available, as it eliminates the need for key hashing and bucket searching. The function requires that the entry be exclusively locked (obtained through  or ) and automatically releases the lock after deletion, similar to . It determines the partition from the item's stored hash value and performs the actual deletion through the internal  function.

## Parameters / Member Variables
- : Pointer to the dynamic shared hash table containing the entry
- : Pointer to the entry to be deleted (must be exclusively locked)

## Dependencies
- Functions called/Symbols referenced:
  - ITEM_FROM_ENTRY: Macro to convert entry pointer to internal item structure
  - PARTITION_FOR_HASH: Macro to determine partition from hash value
  - PARTITION_LOCK: Macro to get partition lock
  - [delete_item](delete_item.md): Internal function that performs the actual deletion
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md): Verifies that the caller holds the required lock
  - [LWLockRelease](../L/LWLockRelease.md): Releases the partition lock
- Called from (representative examples):
  - [pgstat_free_entry](../p/pgstat_free_entry.md): Statistics entry cleanup operations

## Notes and Other Information
- Requires the entry to be obtained through  or  with exclusive locking
- More efficient than  when the entry pointer is already available
- Automatically releases the exclusive lock held on the entry after deletion
- Includes assertions to verify the hash table integrity and lock ownership
- The entry pointer becomes invalid after this function completes
- Uses the hash value stored in the item to determine the correct partition
- No return value as the deletion is always performed (entry is assumed to exist)