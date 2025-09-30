# ResourceOwnerRememberCatCacheRef

## Location
[src/backend/utils/cache/catcache.c:159-163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L159-L163)

## Overview
A convenience wrapper function that registers a catalog cache reference with a resource owner to ensure proper cleanup during transaction abort or error recovery.

## Definition

```c
static inline void
ResourceOwnerRememberCatCacheRef(ResourceOwner owner, HeapTuple tuple)
```
## Detailed Description
ResourceOwnerRememberCatCacheRef is a static inline wrapper function that simplifies the process of registering catalog cache references with PostgreSQL's resource management system. It internally calls ResourceOwnerRemember() with the appropriate resource owner descriptor (catcache_resowner_desc) to track catalog cache tuple references. This ensures that if a transaction aborts or an error occurs, the catalog cache references will be properly released during cleanup. The function is part of PostgreSQL's resource management infrastructure that prevents resource leaks by tracking and automatically cleaning up resources when transactions end abnormally.

## Parameters / Member Variables
- : The ResourceOwner object responsible for tracking this catalog cache reference
- : The HeapTuple from the catalog cache that needs to be tracked for proper cleanup

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerRemember](ResourceOwnerRemember.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - catcache_resowner_desc (static resource owner descriptor)
- Called from (representative examples):
  - [SearchCatCacheInternal](../S/SearchCatCacheInternal.md)
  - [SearchCatCacheMiss](../S/SearchCatCacheMiss.md)

## Notes and Other Information
- This is a static inline function defined in src/backend/utils/cache/catcache.c (lines 159-163)
- Part of a pair with ResourceOwnerForgetCatCacheRef for symmetric resource management
- Uses the catcache_resowner_desc descriptor which has release_phase RESOURCE_RELEASE_AFTER_LOCKS
- Essential for preventing catalog cache reference leaks during error recovery
- The function converts the HeapTuple pointer to a Datum for storage in the resource owner's tracking structures

## Simplified Source

```c
static inline void ResourceOwnerRememberCatCacheRef(ResourceOwner owner, HeapTuple tuple) {
    // Register catalog cache reference with resource owner for automatic cleanup
    ResourceOwnerRemember(owner, PointerGetDatum(tuple), &catcache_resowner_desc);
}
```