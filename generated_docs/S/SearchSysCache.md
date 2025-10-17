# SearchSysCache

## Location
[src/backend/utils/cache/syscache.c:208-220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/syscache.c#L208-L220)

## Overview
A convenience wrapper around SearchCatCache that provides a simplified interface for searching PostgreSQL's system catalog caches with up to 4 search keys.

## Definition

```c
HeapTuple
SearchSysCache(int cacheId,
			   Datum key1,
			   Datum key2,
			   Datum key3,
			   Datum key4)
```
## Detailed Description
SearchSysCache serves as a high-level interface to PostgreSQL's catalog cache system, abstracting away the complexity of cache initialization and key management. It acts as a layer on top of SearchCatCache, automatically handling the cache lookup and reference counting.

The function returns a cache copy of the requested tuple if found, or NULL if no matching tuple exists. The returned tuple is a read-only cache copy that must NOT be modified by the caller. The function automatically increments the reference count for the returned tuple, which must be decremented by calling ReleaseSysCache() when the caller is finished with the tuple.

This is the primary interface used throughout PostgreSQL for accessing cached system catalog information, providing a standardized way to query system metadata.

## Parameters / Member Variables
- `cacheId`: Integer identifier specifying which system cache to search (must be valid cache ID)
- `key1`: First search key value (Datum type for flexibility)
- `key2`: Second search key value (can be unused if cache uses fewer keys)
- `key3`: Third search key value (can be unused if cache uses fewer keys)
- `key4`: Fourth search key value (can be unused if cache uses fewer keys)
## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid
  - [SearchCatCache](SearchCatCache.md)
- Called from (representative examples):
  - [SearchSysCacheCopy](SearchSysCacheCopy.md)
  - [SearchSysCacheExists](SearchSysCacheExists.md)
  - [GetSysCacheOid](../G/GetSysCacheOid.md)

## Notes and Other Information
- The returned tuple is a cache copy and must NEVER be freed by the caller
- Always call ReleaseSysCache() when done with the returned tuple to avoid reference count leaks
- The function validates the cacheId parameter with assertions to ensure cache validity
- Supports up to 4 search keys, with unused keys typically set to appropriate null/default values
- The tuple remains locked in cache until ReleaseSysCache() is called or transaction ends
- This is the most commonly used function for system catalog lookups in PostgreSQL
- Part of the performance-critical path for metadata access in query processing

## Simplified Source

```c
HeapTuple SearchSysCache(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4) {
    // Validate cache ID and ensure cache exists
    Assert(cacheId >= 0 && cacheId < SysCacheSize &&
           PointerIsValid(SysCache[cacheId]));

    // Delegate to underlying catalog cache search
    return SearchCatCache(SysCache[cacheId], key1, key2, key3, key4);
}
```