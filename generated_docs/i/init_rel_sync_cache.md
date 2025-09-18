# init_rel_sync_cache

## Location
[src/backend/replication/pgoutput/pgoutput.c:1917-1970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L1917-L1970)

## Overview
This function initializes the relation schema synchronization cache for a logical decoding session, creating a hash table to track relation metadata and registering necessary callbacks for cache invalidation.

## Definition
```c
static void
init_rel_sync_cache(MemoryContext cachectx)
```

## Detailed Description
The `init_rel_sync_cache` function sets up the relation synchronization cache used during logical replication. This cache stores metadata about relations being replicated to avoid repeated lookups and ensure schema consistency. The function creates a hash table using the provided memory context and registers various callback functions to handle cache invalidation when related system catalogs change. The cache persists for the duration of a decoding session and is automatically cleaned up when the session ends.

## Parameters / Member Variables
- `cachectx`: MemoryContext specifying the memory context in which to create the hash table

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md)
  - [CacheRegisterRelcacheCallback](../C/CacheRegisterRelcacheCallback.md)
  - [CacheRegisterSyscacheCallback](../C/CacheRegisterSyscacheCallback.md)
  - [rel_sync_cache_relation_cb](../r/rel_sync_cache_relation_cb.md)
  - [rel_sync_cache_publication_cb](../r/rel_sync_cache_publication_cb.md)
- Called from (representative examples):
  - [pgoutput_startup](../p/pgoutput_startup.md)

## Notes and Other Information
- The function uses a static boolean `relation_callbacks_registered` to ensure callbacks are registered only once
- If the RelationSyncCache already exists, the function returns early without creating a new one
- The hash table is configured with Oid as the key and RelationSyncEntry as the entry type
- Initial hash table size is set to 128 entries
- Multiple callback types are registered:
  - Relcache callbacks for relation changes
  - Syscache callbacks for NAMESPACEOID (schema renames)
  - Syscache callbacks for PUBLICATIONRELMAP (publication-relation mappings)
  - Syscache callbacks for PUBLICATIONNAMESPACEMAP (publication-namespace mappings)
- The cache is automatically destroyed when the decoding session ends
- Cache invalidations during session will invoke the registered callbacks to maintain consistency
- This function is essential for efficient logical replication performance by avoiding repeated catalog lookups