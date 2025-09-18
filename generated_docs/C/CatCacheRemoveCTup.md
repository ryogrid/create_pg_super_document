# CatCacheRemoveCTup

## Location
src/backend/utils/cache/catcache.c: 528 - 569

## Overview
CatCacheRemoveCTup is a static function that removes and deallocates a catalog cache tuple entry, handling both individual entries and their associated lists.

## Definition


## Detailed Description
This function performs the complete removal of a catalog cache tuple entry from the cache system. It handles the complex cleanup process including deallocation of memory, unlinking from data structures, and maintaining cache statistics. The function includes special logic for handling entries that are part of CatCList structures.

Key operations performed:
- Validates that the entry has zero reference count before removal
- Handles recursive removal of associated CatCList if present
- Delinks the entry from the cache's doubly-linked list
- Frees memory for negative entries' keys (positive entries have keys embedded in tuple data)
- Updates cache tuple counters

The function ensures data integrity by requiring zero reference counts, preventing premature deletion of actively used cache entries.

## Parameters / Member Variables
- : Pointer to the CatCache structure containing the entry to be removed
- : Pointer to the CatCTup (catalog cache tuple) entry to be removed

## Dependencies
- Functions called/Symbols referenced:
  - [CatCacheRemoveCList](CatCacheRemoveCList.md) (for removing associated cache lists)
  - [dlist_delete](../d/dlist_delete.md) (for unlinking from doubly-linked list)
  - [CatCacheFreeKeys](CatCacheFreeKeys.md) (for deallocating negative entry keys)
  - [pfree](../p/pfree.md) (for memory deallocation)
- Called from (representative examples):
  - [CatCacheRemoveCList](CatCacheRemoveCList.md)
  - [CatCacheInvalidate](CatCacheInvalidate.md)
  - [ResetCatalogCache](../R/ResetCatalogCache.md)
  - [ReleaseCatCacheWithOwner](../R/ReleaseCatCacheWithOwner.md)
  - [SearchCatCacheList](../S/SearchCatCacheList.md)

## Notes and Other Information
- Function is declared as static, making it internal to the catcache.c module
- Includes assertions to ensure the entry has zero reference count and belongs to the specified cache
- Uses a "dead" flag mechanism to prevent infinite recursion when removing entries from CatCLists
- Distinguishes between positive and negative cache entries for proper key memory management
- Decrements both cache-specific and global tuple counters to maintain accurate statistics
- Critical for maintaining cache consistency during invalidation and cleanup operations