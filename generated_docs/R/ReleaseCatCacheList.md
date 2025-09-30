# ReleaseCatCacheList

## Location
[src/backend/utils/cache/catcache.c:2073-2078](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L2073-L2078)

## Overview
Decrements the reference count of a catalog cache list, serving as a wrapper function for resource management.

## Definition

```c
void
ReleaseCatCacheList(CatCList *list)
```
## Detailed Description
ReleaseCatCacheList is a simple wrapper function that decrements the reference count of a catalog cache list using the current resource owner. It delegates the actual work to ReleaseCatCacheListWithOwner, passing the CurrentResourceOwner as the owner parameter.

This function is the standard way for code to release a catalog cache list when it no longer needs it. The function ensures proper resource tracking and cleanup, and when the reference count reaches zero, the list may become eligible for removal from the cache.

## Parameters
- : The CatCList to release (decrement reference count)

## Dependencies
- Functions called/Symbols referenced:
  - [ReleaseCatCacheListWithOwner](ReleaseCatCacheListWithOwner.md)
  - CurrentResourceOwner (global variable)
- Called from (representative examples):
  - [brinvalidate](../b/brinvalidate.md)
  - [ginvalidate](../g/ginvalidate.md)  
  - [gistvalidate](../g/gistvalidate.md)
  - [hashvalidate](../h/hashvalidate.md)
  - [btvalidate](../b/btvalidate.md)
  - [spgvalidate](../s/spgvalidate.md)
  - [AddEnumLabel](../A/AddEnumLabel.md)
  - [RenameEnumLabel](RenameEnumLabel.md)
  - [transformFrameOffset](../t/transformFrameOffset.md)
  - ReleaseSysCacheList

## Notes and Other Information
- This function must be called for every CatCList obtained from SearchCatCacheList
- Failure to call this function leads to memory leaks and resource owner violations
- The function is safe to call with NULL pointers (handled by the underlying implementation)
- Part of PostgreSQL's reference counting system for catalog cache management
- Always use this function rather than directly manipulating reference counts

## Simplified Source

```c
void
ReleaseCatCacheList(CatCList *list)
{
    // Simple wrapper that decrements reference count using current resource owner
    // Must be called for every CatCList obtained from SearchCatCacheList
    ReleaseCatCacheListWithOwner(list, CurrentResourceOwner);
}
```