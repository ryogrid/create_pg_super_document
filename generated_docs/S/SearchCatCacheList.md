# SearchCatCacheList

## Location
src/backend/utils/cache/catcache.c: 1697 - 2072

## Overview
Generates a list of all tuples matching a partial key in the catalog cache, supporting searches on just the first K of the cache's N key columns.

## Definition


## Detailed Description
SearchCatCacheList is a core catalog cache function that performs partial key searches to find multiple matching tuples. Unlike SearchCatCache which finds a single tuple using a complete key, this function takes fewer key values and returns all tuples that match the specified prefix of keys.

The function first checks if a matching CatCList already exists in the cache. If found, it moves the list to the front of its hash bucket for faster future access and returns it with an incremented reference count. If not found, it builds a new list by scanning the underlying system catalog, creating or reusing cache entries for each matching tuple.

The implementation includes sophisticated concurrency handling through an "in-progress" mechanism that tracks ongoing list construction to handle cache invalidations that occur during the scan. If invalidation occurs mid-build, the scan is restarted to ensure consistency.

## Parameters
- : The catalog cache to search in
- : Number of key columns to match (must be > 0 and < cache->cc_nkeys)  
- : Value for the first key column
- : Value for the second key column (or dummy value if nkeys < 2)
- : Value for the third key column (or dummy value if nkeys < 3)

## Dependencies
- Functions called/Symbols referenced:
  - CatalogCacheInitializeCache
  - CatalogCacheComputeHashValue
  - CatalogCacheCompareTuple
  - CatalogCacheCreateEntry
  - CatCacheCopyKeys
  - RehashCatCacheLists
  - ResourceOwnerEnlarge
  - ResourceOwnerRememberCatCacheListRef
  - systable_beginscan/systable_getnext
  - table_open/table_close
- Called from (representative examples):
  - SearchSysCacheList
  - Various high-level catalog access functions

## Notes and Other Information
- The caller must not modify the returned list or its tuples
- The caller must call ReleaseCatCacheList() when done with the list
- Automatically handles hash table resizing when the list cache becomes too full
- Uses LRU-style ordering by moving found lists to the front of their hash buckets
- Includes comprehensive error handling with PG_TRY/PG_CATCH blocks
- Supports both index and sequential scans depending on the underlying catalog structure
- The returned list is ordered if an index scan was used during construction