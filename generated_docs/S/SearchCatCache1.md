# SearchCatCache1

## Location
src/backend/utils/cache/catcache.c: 1329 - 1336

## Overview
SearchCatCache1 is an optimized version of SearchCatCache specifically designed for catalog cache searches that require exactly one search key, providing better performance through compiler optimizations.

## Definition
```c
HeapTuple SearchCatCache1(CatCache *cache, Datum v1)
```

## Detailed Description
SearchCatCache1 is a specialized variant of SearchCatCache optimized for cases where exactly one search key is needed. This function is part of a family of SearchCatCacheN() functions that provide type-specific interfaces for different numbers of search arguments. The compiler can inline the function body and unroll loops, making it faster than the general-purpose SearchCatCache() function.

Like its parent function, SearchCatCache1 searches a system catalog cache for a tuple matching the provided search key. It automatically handles cache initialization (opening the underlying relation on first access) and returns either NULL for no match or a pointer to a HeapTuple in the cache. The returned tuple must not be modified and requires ReleaseCatCache() to be called when finished.

## Parameters / Member Variables
- `cache`: Pointer to the CatCache structure representing the specific catalog cache to search
- `v1`: The single search key value as a Datum

## Dependencies
- Functions called/Symbols referenced:
  - SearchCatCacheInternal
  - CatCache (structure type)
- Called from (representative examples):
  - SearchSysCache1
  - CatCacheHeader

## Notes and Other Information
- This is an optimized version that hard-codes the number of keys to 1
- Provides better performance than SearchCatCache() for single-key searches due to compiler optimizations
- Part of the SearchCatCacheN() family (SearchCatCache1, SearchCatCache2, SearchCatCache3, SearchCatCache4)
- Internally calls SearchCatCacheInternal with nkeys=1 and remaining parameters as 0
- The caller must still call ReleaseCatCache() when done with the returned tuple
- Same constraints as SearchCatCache: returned tuple must not be modified