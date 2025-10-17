# ResourceOwnerRememberCatCacheListRef

## Location
[src/backend/utils/cache/catcache.c:169-173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L169-L173)

## Overview
A convenience wrapper function that registers a catalog cache list reference with a resource owner to ensure proper cleanup during transaction abort or error recovery.

## Definition

```c
static inline void
ResourceOwnerRememberCatCacheListRef(ResourceOwner owner, CatCList *list)
```
## Detailed Description
ResourceOwnerRememberCatCacheListRef is a static inline wrapper function that registers catalog cache list references with PostgreSQL's resource management system. Unlike individual catalog cache references, this function tracks CatCList objects, which represent lists of related catalog cache entries returned by SearchCatCacheList operations. It internally calls ResourceOwnerRemember() with the catlistref_resowner_desc descriptor to ensure that catalog cache list references are properly tracked and cleaned up during transaction abort or error recovery. This is essential for preventing memory leaks of catalog cache lists, which can contain multiple tuples and have their own reference counting mechanisms.

## Parameters / Member Variables
- `owner`: The ResourceOwner object responsible for tracking this catalog cache list reference
- `*list`: The CatCList pointer that represents a list of catalog cache entries to be tracked
## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerRemember](ResourceOwnerRemember.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - catlistref_resowner_desc (static resource owner descriptor for catalog cache lists)
- Called from (representative examples):
  - [SearchCatCacheList](../S/SearchCatCacheList.md) (multiple call sites at lines 1809 and 2059)

## Notes and Other Information
- This is a static inline function defined in src/backend/utils/cache/catcache.c (lines 169-173)
- Part of a pair with ResourceOwnerForgetCatCacheListRef for symmetric resource management
- Uses catlistref_resowner_desc descriptor with release_phase RESOURCE_RELEASE_AFTER_LOCKS and priority RELEASE_PRIO_CATCACHE_LIST_REFS
- [CatCList](../C/CatCList.md) objects are more complex than individual catalog cache references as they contain multiple tuples
- Essential for preventing catalog cache list reference leaks during error recovery
- Works in conjunction with the catalog cache list reference counting system
- The function handles CatCList pointers specifically, distinct from individual HeapTuple references

## Simplified Source

```c
static inline void
ResourceOwnerRememberCatCacheListRef(ResourceOwner owner, CatCList *list)
{
    // Register catalog cache list reference for cleanup tracking
    ResourceOwnerRemember(owner, PointerGetDatum(list), &catlistref_resowner_desc);
}
```