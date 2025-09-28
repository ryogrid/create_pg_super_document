# InitCatCache

## Location
[src/backend/utils/cache/catcache.c:878-984](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L878-L984)

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

## Simplified Source

```c
// Simplified version of InitCatCache
CatCache *InitCatCache(int id, Oid reloid, Oid indexoid, int nkeys,
                       const int *key, int nbuckets) {
    CatCache *cp;
    MemoryContext oldcxt;
    int i;

    // Validate nbuckets is power of two
    Assert(nbuckets > 0 && (nbuckets & -nbuckets) == nbuckets);

    // Switch to cache memory context for persistent allocation
    if (!CacheMemoryContext)
        CreateCacheMemoryContext();
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

    // Initialize global cache header if first cache
    if (CacheHdr == NULL) {
        CacheHdr = (CatCacheHeader *) palloc(sizeof(CatCacheHeader));
        slist_init(&CacheHdr->ch_caches);
        CacheHdr->ch_ntup = 0;
    }

    // Allocate cache-line aligned cache structure
    cp = (CatCache *) palloc_aligned(sizeof(CatCache), PG_CACHE_LINE_SIZE,
                                     MCXT_ALLOC_ZERO);
    cp->cc_bucket = palloc0(nbuckets * sizeof(dlist_head));

    // Initialize cache configuration
    cp->id = id;
    cp->cc_reloid = reloid;
    cp->cc_indexoid = indexoid;
    cp->cc_nbuckets = nbuckets;
    cp->cc_nkeys = nkeys;

    // Copy key attribute numbers
    for (i = 0; i < nkeys; ++i) {
        cp->cc_keyno[i] = key[i];
    }

    // Add cache to global list and restore context
    slist_push_head(&CacheHdr->ch_caches, &cp->cc_next);
    MemoryContextSwitchTo(oldcxt);

    return cp;
}
```

Key simplifications made:
- Removed detailed comments explaining power-of-two validation
- Simplified memory context switching logic
- Removed CATCACHE_STATS conditional compilation code
- Consolidated cache field initialization
- Removed temporary field assignments and debugging macros
- Focused on core allocation and initialization flow
- Removed cc_lbucket initialization detail (lazy allocation concept preserved)