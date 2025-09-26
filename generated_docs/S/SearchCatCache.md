# SearchCatCache

## Location
[src/backend/utils/cache/catcache.c:1312-1328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L1312-L1328)

## Overview
SearchCatCache is a core function in PostgreSQL's catalog cache system that searches for a tuple in a system cache, providing efficient access to system catalog information.

## Definition

```c
HeapTuple
SearchCatCache(CatCache *cache,
			   Datum v1,
			   Datum v2,
			   Datum v3,
			   Datum v4)
```
## Detailed Description
SearchCatCache searches a system catalog cache for a tuple matching the provided search key values. This function serves as the primary interface for cache lookups, automatically opening the underlying relation if necessary (on the first access to a particular cache). The function is designed to handle up to 4 search key values, accommodating the various indexing patterns used across PostgreSQL's system catalogs.

The function returns either NULL if no matching tuple is found, or a pointer to a HeapTuple stored in the cache. Importantly, the returned tuple must not be modified by the caller, and ReleaseCatCache() must be called when the caller is done with the tuple to properly manage reference counting.

The search mechanism supports flexible key handling, including a special case for NAME columns where C strings can be passed directly without requiring conversion to fully null-padded NAME format.

## Parameters / Member Variables
- : Pointer to the CatCache structure representing the specific catalog cache to search
- : First search key value as a Datum (or zero if unused)
- : Second search key value as a Datum (or zero if unused)  
- : Third search key value as a Datum (or zero if unused)
- : Fourth search key value as a Datum (or zero if unused)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchCatCacheInternal](SearchCatCacheInternal.md)
  - [CatCache](../C/CatCache.md) (structure type)
- Called from (representative examples):
  - [SearchSysCache](SearchSysCache.md)
  - [CatCacheHeader](../C/CatCacheHeader.md)

## Notes and Other Information
- The function is a wrapper around SearchCatCacheInternal, passing along the cache's configured number of keys
- Search key values should match the datatype of the corresponding key columns
- Unused key parameters should be passed as zero
- Special handling exists for NAME columns - C strings can be passed directly
- The caller is responsible for calling ReleaseCatCache() to release the returned tuple
- The returned tuple should never be modified by the caller
- This function may trigger the opening of the underlying relation on first access to a cache