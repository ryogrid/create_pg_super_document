# ResourceOwnerForgetCatCacheListRef

## Location
[src/backend/utils/cache/catcache.c:174-190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L174-L190)

## Overview
A convenience wrapper function that unregisters a catalog cache list reference from a resource owner when the list reference is explicitly released.

## Definition

```c
static inline void
ResourceOwnerForgetCatCacheListRef(ResourceOwner owner, CatCList *list)
```
## Detailed Description
ResourceOwnerForgetCatCacheListRef is a static inline wrapper function that removes a catalog cache list reference from the resource owner's tracking system. It serves as the counterpart to ResourceOwnerRememberCatCacheListRef, providing symmetric resource management for CatCList objects. When a catalog cache list reference is explicitly released (rather than through error cleanup), this function ensures that the resource owner stops tracking it by calling ResourceOwnerForget() with the catlistref_resowner_desc descriptor. This prevents double-cleanup scenarios and maintains accurate resource accounting for catalog cache lists, which are more complex than individual cache entries as they can contain multiple tuples.

## Parameters / Member Variables
- : The ResourceOwner object that was previously tracking this catalog cache list reference
- : The CatCList pointer that should no longer be tracked by the resource owner

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerForget](ResourceOwnerForget.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - catlistref_resowner_desc (static resource owner descriptor for catalog cache lists)
- Called from (representative examples):
  - [ReleaseCatCacheListWithOwner](ReleaseCatCacheListWithOwner.md)

## Notes and Other Information
- This is a static inline function defined in src/backend/utils/cache/catcache.c (lines 174-178)
- Forms a symmetric pair with ResourceOwnerRememberCatCacheListRef for complete resource lifecycle management
- Uses the catlistref_resowner_desc descriptor with release priority RELEASE_PRIO_CATCACHE_LIST_REFS
- Called during normal catalog cache list release operations, not during error cleanup
- Essential for maintaining accurate resource tracking of CatCList objects and preventing resource leaks or double-frees
- Works specifically with CatCList pointers, which are distinct from individual catalog cache tuple references
- Part of PostgreSQL's comprehensive resource management system for catalog cache operations

## Simplified Source

```c
static inline void ResourceOwnerForgetCatCacheListRef(ResourceOwner owner, CatCList *list) {
    // Remove catalog cache list reference from resource owner tracking
    ResourceOwnerForget(owner, PointerGetDatum(list), &catlistref_resowner_desc);
}
```