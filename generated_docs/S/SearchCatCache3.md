# SearchCatCache3

## Location
[src/backend/utils/cache/catcache.c:1345-1352](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L1345-L1352)

## Overview
SearchCatCache3 is an optimized version of SearchCatCache specifically designed for catalog cache searches that require exactly three search keys, providing better performance through compiler optimizations.

## Definition
```c
HeapTuple SearchCatCache3(CatCache *cache, Datum v1, Datum v2, Datum v3)
```

## Detailed Description
SearchCatCache3 is a specialized variant of SearchCatCache optimized for cases where exactly three search keys are needed. This function is part of the SearchCatCacheN() family that provides type-specific interfaces for different numbers of search arguments. The compiler can inline the function body and unroll loops, making it faster than the general-purpose SearchCatCache() function.

Like other functions in this family, SearchCatCache3 searches a system catalog cache for a tuple matching the provided search keys. It handles cache initialization automatically (opening the underlying relation on first access) and returns either NULL for no match or a pointer to a HeapTuple in the cache. The returned tuple must not be modified and requires ReleaseCatCache() to be called when finished.

## Parameters / Member Variables
- `cache`: Pointer to the CatCache structure representing the specific catalog cache to search
- `v1`: The first search key value as a Datum
- `v2`: The second search key value as a Datum
- `v3`: The third search key value as a Datum

## Dependencies
- Functions called/Symbols referenced:
  - [SearchCatCacheInternal](SearchCatCacheInternal.md)
  - [CatCache](../C/CatCache.md) (structure type)
- Called from (representative examples):
  - [SearchSysCache3](SearchSysCache3.md)
  - [CatCacheHeader](../C/CatCacheHeader.md)

## Notes and Other Information
- This is an optimized version that hard-codes the number of keys to 3
- Provides better performance than SearchCatCache() for three-key searches due to compiler optimizations
- Part of the SearchCatCacheN() family (SearchCatCache1, SearchCatCache2, SearchCatCache3, SearchCatCache4)
- Internally calls SearchCatCacheInternal with nkeys=3 and the fourth parameter as 0
- The caller must still call ReleaseCatCache() when done with the returned tuple
- Same constraints as SearchCatCache: returned tuple must not be modified
- Commonly used for catalog lookups that require complex compound keys, such as searches involving namespace, object name, and type information