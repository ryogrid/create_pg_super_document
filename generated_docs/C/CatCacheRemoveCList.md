# CatCacheRemoveCList

## Location
[src/backend/utils/cache/catcache.c:570-624](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L570-L624)

## Overview
CatCacheRemoveCList is a static function that removes and deallocates a catalog cache list entry along with any unreferenced member entries.

## Definition


## Detailed Description
This function handles the complete removal of a CatCList (catalog cache list) structure from the cache system. CatCLists are used to cache the results of searches that return multiple tuples matching the same key values. The function performs a complex cleanup process that includes managing the relationships between the list and its member cache entries.

Key operations performed:
- Validates that the list has zero reference count before removal
- Iterates through all member cache entries and clears their list associations
- Removes any member entries that are marked as "dead" and have zero references
- Delinks the list from the cache's doubly-linked list structure
- Frees the memory allocated for the list's key values
- Updates the cache's list counter

The function handles both normal operation and CATCACHE_FORCE_RELEASE mode, where in the latter case member entries are removed regardless of their dead status.

## Parameters / Member Variables
- : Pointer to the CatCache structure containing the list to be removed
- : Pointer to the CatCList entry to be removed

## Dependencies
- Functions called/Symbols referenced:
  - [CatCacheRemoveCTup](CatCacheRemoveCTup.md) (for removing unreferenced member entries)
  - [dlist_delete](../d/dlist_delete.md) (for unlinking from doubly-linked list)
  - [CatCacheFreeKeys](CatCacheFreeKeys.md) (for deallocating key memory)
  - [pfree](../p/pfree.md) (for memory deallocation)
- Called from (representative examples):
  - [CatCacheRemoveCTup](CatCacheRemoveCTup.md) (recursive relationship)
  - [CatCacheInvalidate](CatCacheInvalidate.md)
  - [ResetCatalogCache](../R/ResetCatalogCache.md)
  - [ReleaseCatCacheListWithOwner](../R/ReleaseCatCacheListWithOwner.md)

## Notes and Other Information
- Function is declared as static, making it internal to the catcache.c module
- Includes assertions to ensure the list has zero reference count and belongs to the specified cache
- Contains conditional compilation logic for CATCACHE_FORCE_RELEASE mode
- Works in conjunction with CatCacheRemoveCTup to handle mutual dependencies between lists and their members
- Critical for maintaining cache consistency during list invalidation and cleanup operations
- Member tuple removal is conditional on both dead status and zero reference count to prevent premature deletion