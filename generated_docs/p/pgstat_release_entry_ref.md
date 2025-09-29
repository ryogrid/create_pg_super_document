# pgstat_release_entry_ref

## Location
[src/backend/utils/activity/pgstat_shmem.c:550-620](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L550-L620)

## Overview
Releases a reference to a shared statistics entry and performs cleanup when the reference count reaches zero.

## Definition
static void pgstat_release_entry_ref(PgStat_HashKey key, PgStat_EntryRef *entry_ref, bool discard_pending)

## Detailed Description
This static function manages the cleanup of statistics entry references in PostgreSQL's shared statistics system. It handles both pending statistics data and shared statistics references. When discard_pending is false and pending data exists, it raises an error to prevent data loss. The function uses atomic operations to decrement the reference count on the shared entry, and when the count reaches zero, it attempts to remove the shared entry from the hash table. It implements generation checking to handle concurrent reinitialization of entries, ensuring that only entries from the same generation are actually freed. The function also removes the local reference from the entry reference hash table and frees the local memory.

## Parameters / Member Variables
- : Hash key identifying the statistics entry
- : Local reference to the statistics entry being released
- : Whether to discard pending statistics data or error if present

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_delete_pending_entry](pgstat_delete_pending_entry.md)
  - [pg_atomic_fetch_sub_u32](pg_atomic_fetch_sub_u32.md)
  - [dshash_find](../d/dshash_find.md)
  - [pg_atomic_read_u32](pg_atomic_read_u32.md)
  - [pgstat_free_entry](pgstat_free_entry.md)
  - [dshash_release_lock](../d/dshash_release_lock.md)
  - pgstat_entry_ref_hash_delete
  - [pfree](pfree.md)
- Called from (representative examples):
  - [pgstat_get_entry_ref](pgstat_get_entry_ref.md)
  - [pgstat_gc_entry_refs](pgstat_gc_entry_refs.md)
  - [pgstat_release_matching_entry_refs](pgstat_release_matching_entry_refs.md)
  - [pgstat_drop_entry](pgstat_drop_entry.md)

## Notes and Other Information
The function implements careful synchronization to handle concurrent access to shared entries. The generation checking prevents race conditions when entries are reused. Only dropped entries can reach a zero reference count, which is enforced by assertions.

## Simplified Source

```c
static void
pgstat_release_entry_ref(PgStat_HashKey key, PgStat_EntryRef *entry_ref,
                        bool discard_pending)
{
    // Handle pending statistics data
    if (entry_ref && entry_ref->pending)
    {
        if (discard_pending)
            pgstat_delete_pending_entry(entry_ref);
        else
            elog(ERROR, "releasing ref with pending data");
    }

    // Handle shared statistics reference
    if (entry_ref && entry_ref->shared_stats)
    {
        Assert(entry_ref->shared_stats->magic == 0xdeadbeef);
        Assert(entry_ref->pending == NULL);

        // Decrement reference count atomically
        if (pg_atomic_fetch_sub_u32(&entry_ref->shared_entry->refcount, 1) == 1)
        {
            // We're the last reference - try to drop the shared entry
            PgStatShared_HashEntry *shent;

            Assert(entry_ref->shared_entry->dropped);

            // Find and lock the shared entry
            shent = dshash_find(pgStatLocal.shared_hash,
                               &entry_ref->shared_entry->key,
                               true);
            if (!shent)
                elog(ERROR, "could not find just referenced shared stats entry");

            // Check if entry was reinitialized (generation check)
            if (pg_atomic_read_u32(&entry_ref->shared_entry->generation) ==
                entry_ref->generation)
            {
                // Same generation - safe to free
                Assert(pg_atomic_read_u32(&entry_ref->shared_entry->refcount) == 0);
                Assert(entry_ref->shared_entry == shent);
                pgstat_free_entry(shent, NULL);
            }
            else
            {
                // Entry was reinitialized - just release lock
                dshash_release_lock(pgStatLocal.shared_hash, shent);
            }
        }
    }

    // Remove local reference from hash table
    if (!pgstat_entry_ref_hash_delete(pgStatEntryRefHash, key))
        elog(ERROR, "entry ref vanished before deletion");

    // Free local memory
    if (entry_ref)
        pfree(entry_ref);
}
```