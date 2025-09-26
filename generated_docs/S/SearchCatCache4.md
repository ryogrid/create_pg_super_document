# SearchCatCache4

## Location
[src/backend/utils/cache/catcache.c:1353-1362](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L1353-L1362)

## Overview
SearchCatCache4 is an optimized version of SearchCatCache specifically designed for catalog cache searches that require exactly four search keys, providing better performance through compiler optimizations.

## Definition
```c
HeapTuple SearchCatCache4(CatCache *cache, Datum v1, Datum v2, Datum v3, Datum v4)
```

## Detailed Description
SearchCatCache4 is a specialized variant of SearchCatCache optimized for cases where exactly four search keys are needed. This function completes the SearchCatCacheN() family that provides type-specific interfaces for different numbers of search arguments (1-4 keys). The compiler can inline the function body and unroll loops, making it faster than the general-purpose SearchCatCache() function.

Like other functions in this family, SearchCatCache4 searches a system catalog cache for a tuple matching the provided search keys. It handles cache initialization automatically (opening the underlying relation on first access) and returns either NULL for no match or a pointer to a HeapTuple in the cache. The returned tuple must not be modified and requires ReleaseCatCache() to be called when finished.

This function represents the maximum number of search keys supported by PostgreSQL's catalog cache system, as four keys accommodate the most complex indexing patterns used across system catalogs.

## Parameters / Member Variables
- `cache`: Pointer to the CatCache structure representing the specific catalog cache to search
- `v1`: The first search key value as a Datum
- `v2`: The second search key value as a Datum
- `v3`: The third search key value as a Datum
- `v4`: The fourth search key value as a Datum

## Dependencies
- Functions called/Symbols referenced:
  - [SearchCatCacheInternal](SearchCatCacheInternal.md)
  - [CatCache](../C/CatCache.md) (structure type)
- Called from (representative examples):
  - [SearchSysCache4](SearchSysCache4.md)
  - [CatCacheHeader](../C/CatCacheHeader.md)

## Notes and Other Information
- This is an optimized version that hard-codes the number of keys to 4 (the maximum supported)
- Provides better performance than SearchCatCache() for four-key searches due to compiler optimizations
- Completes the SearchCatCacheN() family (SearchCatCache1, SearchCatCache2, SearchCatCache3, SearchCatCache4)
- Internally calls SearchCatCacheInternal with nkeys=4 using all four provided key values
- The caller must still call ReleaseCatCache() when done with the returned tuple
- Same constraints as SearchCatCache: returned tuple must not be modified
- Represents the most complex catalog lookup pattern, typically used for catalogs with highly specific compound keys
- Functionally equivalent to the general SearchCatCache() when all four keys are used, but with better performance characteristics