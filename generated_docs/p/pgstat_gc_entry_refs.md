# pgstat_gc_entry_refs

## Location
[src/backend/utils/activity/pgstat_shmem.c:696-736](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L696-L736)

## Overview
A static function that performs garbage collection of statistics entry references by iterating through the local hash table and removing stale or dropped entries.

## Definition

```c
static void
pgstat_gc_entry_refs(void)
```
## Detailed Description
This function performs the actual garbage collection of statistics entry references. It reads the current GC request count from shared memory and iterates through all entries in the local entry reference hash table (). For each entry, it checks whether the entry has been dropped or reinitialized by comparing generation numbers and checking the dropped flag.

The function uses several criteria to determine if an entry should be garbage collected:
1. The shared entry has been marked as dropped
2. The generation number has changed (indicating reinitialization)
3. The entry has no pending data

Entries that meet the removal criteria are released using . After cleanup, the function updates the local  to match the current GC request count, indicating that GC has been performed up to this point.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  -  - Atomically reads the GC request count from shared memory
  -  - Atomically reads entry generation numbers
  -  - Starts iteration over the entry reference hash table
  -  - Gets the next entry during hash table iteration
  -  - Releases and cleans up an entry reference
  -  - [Hash](../H/Hash.md) table entry structure
  -  - Statistics entry reference structure
  -  - Local hash table of entry references
  -  - Local variable tracking reference age

- Called from (representative examples):
  -  - [Hash](../H/Hash.md) table declaration macro that may reference this function
  -  - Calls this function when GC is needed

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Contains assertions to verify data integrity (magic numbers and proper initialization)
- Skips entries that have pending data to avoid data loss
- Uses atomic operations for thread-safe access to shared memory
- The generation-based approach helps detect when entries have been reinitialized rather than just dropped
- Updates the local reference age after successful cleanup to avoid unnecessary future GC attempts
- Located in src/backend/utils/activity/pgstat_shmem.c

## Simplified Source

```c
static void
pgstat_gc_entry_refs(void)
{
    pgstat_entry_ref_hash_iterator i;
    PgStat_EntryRefHashEntry *ent;
    uint64 curage;

    // Get current GC request count from shared memory
    curage = pg_atomic_read_u64(&pgStatLocal.shmem->gc_request_count);
    Assert(curage != 0);

    // Iterate through all entries in the local reference hash table
    pgstat_entry_ref_hash_start_iterate(pgStatEntryRefHash, &i);
    while ((ent = pgstat_entry_ref_hash_iterate(pgStatEntryRefHash, &i)) != NULL)
    {
        PgStat_EntryRef *entry_ref = ent->entry_ref;

        // Verify entry integrity
        Assert(!entry_ref->shared_stats ||
               entry_ref->shared_stats->magic == 0xdeadbeef);

        // Check if entry is still valid (not dropped and generation matches)
        if (!entry_ref->shared_entry->dropped &&
            pg_atomic_read_u32(&entry_ref->shared_entry->generation) ==
            entry_ref->generation)
            continue;  // Entry is still valid, keep it

        // Cannot garbage collect entries with pending data
        if (entry_ref->pending != NULL)
            continue;

        // Entry is stale - release it
        pgstat_release_entry_ref(ent->key, entry_ref, false);
    }

    // Update local reference age to current GC count
    pgStatSharedRefAge = curage;
}
``` 