# dshash_detach

## Location
[src/backend/lib/dshash.c:307-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L307-L322)

## Overview
Detaches from a dynamic shared hash table by freeing the backend-local resources while leaving the shared hash table data intact for other processes.

## Definition

```c
void
dshash_detach(dshash_table *hash_table)
```
## Detailed Description
The dshash_detach function cleanly disconnects a backend from a shared hash table by releasing only the backend-local dshash_table structure. This is a lightweight operation that does not affect the shared hash table data or control structures, which remain available for other attached processes. The function includes debug assertions to ensure no partition locks are held by the current backend when detaching.

The shared hash table continues to exist after detachment and can be accessed by other backends that remain attached or by new backends that attach using the table handle. The shared data is only freed when the hash table is explicitly destroyed or when the entire dynamic shared area is deallocated.

## Parameters / Member Variables
- : Pointer to the backend-local dshash_table structure to detach from

## Dependencies
- Functions called/Symbols referenced:
  - ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME (debug assertion macro)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [StatsShmemInit](../S/StatsShmemInit.md)
  - [pgstat_detach_shmem](../p/pgstat_detach_shmem.md)
  - [shared_record_typmod_registry_detach](../s/shared_record_typmod_registry_detach.md)

## Notes and Other Information
- Only frees backend-local memory, shared hash table data remains intact
- Safe to call even if the shared hash table has been destroyed by another backend
- Debug builds verify that no partition locks are held before detaching
- Does not require any locks or synchronization with other backends
- The hash table handle remains valid and can be used by other processes to attach

## Simplified Source

```c
// Simplified version of dshash_detach
void dshash_detach(dshash_table *hash_table) {
    // Core logic: Verify no locks held and free backend-local memory
    ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME(hash_table);

    // Free the backend-local hash table structure
    pfree(hash_table);
}
```

Key simplifications made:
- Removed comment about potentially destroyed hash table
- Focused on the two essential operations: debug assertion and memory cleanup
- Consolidated the logic into clear, simple steps
- Maintained critical safety check through assertion