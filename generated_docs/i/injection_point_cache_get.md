# injection_point_cache_get

## Location
src/backend/utils/misc/injection_point.c: 209 - 231

## Overview
Retrieves an injection point entry from the local backend cache by name, returning NULL if not found or cache is uninitialized.

## Definition
```c
static InjectionPointCacheEntry *
injection_point_cache_get(const char *name)
```

## Detailed Description
This function performs a lookup in the local injection point cache hash table to retrieve a previously cached injection point entry. It first checks if the cache has been initialized (InjectionPointCache != NULL), and if not, returns NULL immediately. If the cache exists, it performs a hash table search using the HASH_FIND operation. The function returns the cache entry if found, or NULL if the injection point is not in the cache.

## Parameters / Member Variables
- `name`: Name of the injection point to look up in the cache (up to INJ_NAME_MAXLEN=64 characters)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md): Performs hash table lookup with HASH_FIND flag
  - InjectionPointCache: Global hash table variable for the cache
- Called from (representative examples):
  - [InjectionPointCacheRefresh](../I/InjectionPointCacheRefresh.md): Checks if injection points are already cached during refresh operations

## Notes and Other Information
- The function is static (internal to injection_point.c)
- Returns NULL if the cache hasn't been initialized yet (first injection point not loaded)
- Returns NULL if the requested injection point name is not found in the cache
- Does not modify the cache or trigger any loading operations
- Simple lookup operation with no side effects
- Used primarily during cache validation and refresh processes