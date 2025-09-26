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