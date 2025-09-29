# pgstat_get_entry_ref_cached

## Location
[src/backend/utils/activity/pgstat_shmem.c:362-417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L362-L417)

## Overview
Helper function that manages the local cache for statistics entry references, handling cache hits, misses, and entry allocation for PostgreSQL's statistics collection system.

## Definition

```c
struct PgStat_HashKey));
```
## Detailed Description
The `pgstat_get_entry_ref_cached` function is a critical caching mechanism that optimizes access to shared memory statistics entries. It manages a local hash table cache to avoid repeated lookups of the same entries and reduces contention on the shared hash table.

The function implements an optimized caching strategy:

1. **Proactive cache insertion**: Always inserts a cache entry immediately to avoid multiple hash table lookups and handle out-of-memory situations gracefully
2. **Cache miss handling**: When no valid cached entry exists, allocates a new `PgStat_EntryRef` structure and initializes it with NULL values
3. **Cache validation**: Verifies that cached entries are still valid by checking for non-NULL shared pointers
4. **Reference integrity**: Performs assertions to ensure cached references maintain proper state and reference counts

The function returns true for cache hits (when a valid cached entry exists) and false for cache misses (requiring a fresh lookup from shared memory).

## Parameters / Member Variables
- `key`: Hash key identifying the specific statistics entry to look up in the cache
- `entry_ref_p`: Output parameter that receives a pointer to the `PgStat_EntryRef` structure (either cached or newly allocated)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_entry_ref_hash_insert
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [pg_atomic_read_u32](pg_atomic_read_u32.md)
- Called from (representative examples):
  - [pgstat_get_entry_ref](pgstat_get_entry_ref.md)

## Notes and Other Information
- This is a static helper function, only accessible within the pgstat_shmem.c module
- Uses the `pgStatSharedRefContext` memory context for allocating entry reference structures
- The proactive insertion strategy prevents race conditions and simplifies error handling
- Cache entries with NULL `shared_stats` are considered invalid and trigger cache misses
- Includes comprehensive assertions to verify the integrity of cached references
- The magic number check (0xdeadbeef) ensures cached pointers still point to valid statistics entries
- Critical for performance optimization by reducing shared memory access frequency
- The `PG_USED_FOR_ASSERTS_ONLY` annotation indicates variables used only in assertion builds

## Simplified Source

```c
static bool
pgstat_get_entry_ref_cached(PgStat_HashKey key, PgStat_EntryRef **entry_ref_p)
{
    bool found;
    PgStat_EntryRefHashEntry *cache_entry;

    // Always insert cache entry to avoid multiple lookups and handle OOM gracefully
    cache_entry = pgstat_entry_ref_hash_insert(pgStatEntryRefHash, key, &found);

    if (!found || !cache_entry->entry_ref)
    {
        // Cache miss - allocate new entry reference
        PgStat_EntryRef *entry_ref;

        cache_entry->entry_ref = entry_ref =
            MemoryContextAlloc(pgStatSharedRefContext, sizeof(PgStat_EntryRef));

        // Initialize new entry reference with NULL values
        entry_ref->shared_stats = NULL;
        entry_ref->shared_entry = NULL;
        entry_ref->pending = NULL;

        found = false;
    }
    else if (cache_entry->entry_ref->shared_stats == NULL)
    {
        // Entry exists but stats pointer is NULL - treat as cache miss
        Assert(cache_entry->entry_ref->pending == NULL);
        found = false;
    }
    else
    {
        // Cache hit - validate cached entry
        PgStat_EntryRef *entry_ref PG_USED_FOR_ASSERTS_ONLY;

        entry_ref = cache_entry->entry_ref;
        Assert(entry_ref->shared_entry != NULL);
        Assert(entry_ref->shared_stats != NULL);
        Assert(entry_ref->shared_stats->magic == 0xdeadbeef);
        Assert(pg_atomic_read_u32(&entry_ref->shared_entry->refcount) > 0);
    }

    *entry_ref_p = cache_entry->entry_ref;
    return found;  // true for cache hit, false for cache miss
}
```