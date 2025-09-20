# ReleaseCatCache

## Location
[src/backend/utils/cache/catcache.c:1624-1629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L1624-L1629)

## Overview
Decrements the reference count of a catalog cache entry, releasing the hold acquired by a successful SearchCatCache operation.

## Definition

```c
void
ReleaseCatCache(HeapTuple tuple)
```
## Detailed Description
ReleaseCatCache is a simple wrapper function that decrements the reference count of a catalog cache entry. It delegates the actual work to ReleaseCatCacheWithOwner, passing the current resource owner. This function is called to release a reference to a cache entry that was previously obtained through SearchCatCache or related functions. When the reference count reaches zero, the cache entry becomes eligible for removal from the cache.

The function serves as the standard interface for releasing cache references without needing to specify a resource owner explicitly. It's part of PostgreSQL's reference counting mechanism that ensures cache entries remain valid while being used and can be safely removed when no longer needed.

## Parameters / Member Variables
- : HeapTuple pointer representing the cached catalog tuple to release

## Dependencies
- Functions called/Symbols referenced:
  - [ReleaseCatCacheWithOwner](ReleaseCatCacheWithOwner.md)
  - CurrentResourceOwner
- Called from (representative examples):
  - [ReleaseSysCache](ReleaseSysCache.md)
  - CatCacheHeader

## Notes and Other Information
- Simple wrapper around ReleaseCatCacheWithOwner using CurrentResourceOwner
- Must be called for every successful SearchCatCache operation to avoid reference leaks
- When compiled with CATCACHE_FORCE_RELEASE, entries are freed immediately when refcount reaches zero
- Part of PostgreSQL's resource management system for tracking cache entry usage
- Essential for preventing memory leaks in long-running transactions
- The tuple parameter should be the exact HeapTuple returned by a SearchCatCache call