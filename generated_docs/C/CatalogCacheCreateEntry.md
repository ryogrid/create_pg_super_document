# CatalogCacheCreateEntry

## Location
[src/backend/utils/cache/catcache.c:2113-2260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L2113-L2260)

## Overview
Creates a new catalog cache entry (CatCTup) from a HeapTuple or cache keys, handling both positive and negative cache entries with proper memory management.

## Definition

```c
static CatCTup *
CatalogCacheCreateEntry(CatCache *cache, HeapTuple ntp, Datum *arguments,
						uint32 hashValue, Index hashIndex)
```
## Detailed Description
CatalogCacheCreateEntry is responsible for creating new entries in the catalog cache. It handles two types of entries: positive entries (from actual tuples found in system catalogs) and negative entries (recording that a search found no matching tuple).

For positive entries, the function processes the provided HeapTuple by flattening any TOAST-ed (out-of-line) values to protect against stale references. It allocates memory for both the CatCTup structure and the tuple data in one contiguous block in CacheMemoryContext, then extracts and stores the key values from the tuple.

For negative entries (when ntp is NULL), it creates a minimal CatCTup with just the search keys copied using CatCacheCopyKeys.

The function includes sophisticated handling of concurrent invalidations through the "in-progress" mechanism and includes optional random failure injection in debug builds to test retry logic.

## Parameters
- : The catalog cache to create the entry in
- : The HeapTuple to cache (NULL for negative entries)
- : Cache keys to use for negative entries (unused for positive entries)
- : Pre-computed hash value for the entry
- : Hash bucket index where the entry will be placed

## Dependencies
- Functions called/Symbols referenced:
  - [toast_flatten_tuple](../t/toast_flatten_tuple.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [heap_getattr](../h/heap_getattr.md)
  - [CatCacheCopyKeys](CatCacheCopyKeys.md)
  - [dlist_push_head](../d/dlist_push_head.md)
  - [RehashCatCache](../R/RehashCatCache.md)
  - HeapTupleHasExternal
  - [pg_prng_uint32](../p/pg_prng_uint32.md) (debug builds only)
- Called from (representative examples):
  - [SearchCatCacheMiss](../S/SearchCatCacheMiss.md)
  - [SearchCatCacheList](../S/SearchCatCacheList.md)

## Notes and Other Information
- Returns NULL if the tuple becomes stale during TOAST decompression (caller must retry)
- Creates entries with initial refcount of 0 - caller must increment if needed
- Automatically triggers hash table expansion when load factor exceeds 2
- Includes random failure injection in debug builds to test error paths
- Static function - only callable from within catcache.c
- Memory allocated in CacheMemoryContext for persistence across transactions
- Handles both by-value and by-reference key types correctly
- Part of PostgreSQL's catalog cache invalidation and consistency mechanism