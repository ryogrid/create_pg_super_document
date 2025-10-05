# rel_sync_cache_publication_cb

## Location
[src/backend/replication/pgoutput/pgoutput.c:2380-2405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L2380-L2405)

## Overview
A syscache invalidation callback function that invalidates all relation sync cache entries when publication-related system catalog changes occur.

## Definition

```c
static void
rel_sync_cache_publication_cb(Datum arg, int cacheid, uint32 hashvalue)
```
## Detailed Description
This function serves as a syscache invalidation callback that responds to changes in publication-related system catalogs. It is registered to handle invalidations for multiple system catalogs including pg_publication, pg_publication_rel, pg_publication_namespace, and pg_namespace.

When any of these catalogs are modified (such as when publications are created, dropped, or modified, or when relations are added/removed from publications), this callback is triggered to maintain cache consistency.

Since the function cannot easily determine which specific cache entries are affected by a particular invalidation event, it takes a conservative approach and marks all entries in the RelationSyncCache as invalid. This ensures correctness at the cost of potentially invalidating more entries than strictly necessary.

The actual rebuilding of invalidated entries happens lazily on the next access via get_rel_sync_entry().

## Parameters / Member Variables
- `arg`: Datum argument (unused in this implementation, required by callback signature)
- `cacheid`: The system cache ID that was invalidated (identifies which catalog changed)
- `hashvalue`: The hash value of the invalidated cache entry (unused in this implementation)
## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md) (initialize hash table iteration)
  - [hash_seq_search](../h/hash_seq_search.md) (iterate through all hash entries)
- Called from (representative examples):
  - [publication_invalidation_cb](../p/publication_invalidation_cb.md) (wrapper for publication invalidations)
  - [init_rel_sync_cache](../i/init_rel_sync_cache.md) (registered for multiple syscache invalidations)

## Notes and Other Information
- Registered for multiple system catalogs: pg_publication, pg_publication_rel, pg_publication_namespace, pg_namespace
- Uses a "mark all invalid" approach due to difficulty in determining specific affected entries
- Includes defensive check for RelationSyncCache existence to handle plugin cleanup scenarios
- Works in conjunction with the publications_valid global flag to manage publication reload
- The conservative invalidation strategy ensures correctness in complex publication scenarios
- Invalidation can occur when publications are created, dropped, modified, or when schema/relation membership changes
- Does not perform immediate cleanup - relies on lazy rebuilding during next access

## Simplified Source

```c
static void
rel_sync_cache_publication_cb(Datum arg, int cacheid, uint32 hashvalue)
{
    HASH_SEQ_STATUS status;
    RelationSyncEntry *entry;

    // Early exit if cache doesn't exist
    if (RelationSyncCache == NULL)
        return;

    // Invalidate all entries since we can't determine specific affected ones
    hash_seq_init(&status, RelationSyncCache);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        entry->replicate_valid = false;
    }
}
```