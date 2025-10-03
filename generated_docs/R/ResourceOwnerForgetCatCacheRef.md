# ResourceOwnerForgetCatCacheRef

## Location
[src/backend/utils/cache/catcache.c:164-168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L164-L168)

## Overview
A convenience wrapper function that unregisters a catalog cache reference from a resource owner when the reference is explicitly released.

## Definition

```c
static inline void
ResourceOwnerForgetCatCacheRef(ResourceOwner owner, HeapTuple tuple)
```
## Detailed Description
ResourceOwnerForgetCatCacheRef is a static inline wrapper function that removes a catalog cache reference from the resource owner's tracking system. It serves as the counterpart to ResourceOwnerRememberCatCacheRef, providing symmetric resource management. When a catalog cache reference is explicitly released (rather than through error cleanup), this function ensures that the resource owner stops tracking it by calling ResourceOwnerForget() with the appropriate resource descriptor. This prevents double-cleanup scenarios and maintains accurate resource accounting within PostgreSQL's resource management framework.

## Parameters / Member Variables
- `owner`: The ResourceOwner object that was previously tracking this catalog cache reference
- `tuple`: The HeapTuple from the catalog cache that should no longer be tracked
## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerForget](ResourceOwnerForget.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - catcache_resowner_desc (static resource owner descriptor)
- Called from (representative examples):
  - [ReleaseCatCacheWithOwner](ReleaseCatCacheWithOwner.md)

## Notes and Other Information
- This is a static inline function defined in src/backend/utils/cache/catcache.c (lines 164-168)
- Forms a symmetric pair with ResourceOwnerRememberCatCacheRef for complete resource lifecycle management
- Uses the same catcache_resowner_desc descriptor as its Remember counterpart
- Called during normal catalog cache reference release operations, not during error cleanup
- Essential for maintaining accurate resource tracking and preventing resource leaks or double-frees
- The function converts the HeapTuple pointer to a Datum for lookup in the resource owner's tracking structures