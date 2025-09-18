# InitCatCache

## Location
src/backend/utils/cache/catcache.c: 878 - 984

## Overview
Initializes a new catalog cache structure with specified parameters for caching tuples from a system catalog.

## Definition
```c
CatCache *InitCatCache(int id, Oid reloid, Oid indexoid, int nkeys, const int *key, int nbuckets)
```

## Detailed Description
InitCatCache creates and initializes a new catalog cache structure for caching tuples from a specified system catalog. The function sets up all necessary data structures including hash buckets for efficient tuple lookup, initializes cache metadata, and registers the cache with the global cache management system.

The function performs several key initialization tasks: validates that the number of buckets is a power of two, switches to the cache memory context to ensure cache data persists across transactions, initializes the global cache header if this is the first cache being created, allocates and configures the cache structure with appropriate alignment for performance, and registers the cache in the global cache list.

The cache is initially created without list search buckets (cc_lbucket), which are allocated only when needed to optimize memory usage for caches that never perform list searches.

## Parameters / Member Variables
- `id`: Unique identifier for this cache
- `reloid`: OID of the relation (system catalog) this cache will store tuples from
- `indexoid`: OID of the index to be used for lookups in this cache
- `nkeys`: Number of key attributes for cache lookups
- `key`: Array of attribute numbers that form the cache key
- `nbuckets`: Initial number of hash buckets (must be power of two)

## Dependencies
- Functions called/Symbols referenced:
  - [CreateCacheMemoryContext](../C/CreateCacheMemoryContext.md)
  - [palloc_aligned](../p/palloc_aligned.md)
  - [palloc0](../p/palloc0.md)
  - [slist_init](../s/slist_init.md)
  - [slist_push_head](../s/slist_push_head.md)
  - [on_proc_exit](../o/on_proc_exit.md)
  - [CatCachePrintStats](../C/CatCachePrintStats.md)
  - AttributeNumberIsValid
  - InitCatCache_DEBUG2
- Called from (representative examples):
  - [InitCatalogCache](InitCatalogCache.md)
  - Referenced in CatCacheHeader

## Notes and Other Information
- Returns a pointer to the newly created CatCache structure
- Requires nbuckets to be a power of two for efficient hash distribution
- Uses cache-line aligned allocation for performance optimization
- Initializes cache in CacheMemoryContext to ensure persistence across transactions
- Sets up global cache header on first cache creation
- Supports optional statistics collection when CATCACHE_STATS is enabled
- [List](../L/List.md) search buckets (cc_lbucket) are allocated lazily only when needed
- All dlist headers are initialized correctly through zero-initialization