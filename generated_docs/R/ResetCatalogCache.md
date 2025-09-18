# ResetCatalogCache

## Location
src/backend/utils/cache/catcache.c: 736 - 797

## Overview
Resets a single catalog cache to empty state by removing or marking dead all tuples and lists in the cache.

## Definition


## Detailed Description
ResetCatalogCache is a static function that empties a single catalog cache by iterating through all hash buckets and either removing cached entries or marking them as dead if they are still referenced. The function handles both cached tuples (CatCTup) and cached lists (CatCList). 

When an entry has a reference count greater than 0, it cannot be immediately removed, so it is marked as 'dead' instead. Dead entries will be cleaned up later when their reference count drops to zero.

The function also handles in-progress cache builds, marking them as dead unless this is being called for debug purposes (debug_discard mode), in which case in-progress builds are left alone to allow cache testing to make progress.

## Parameters / Member Variables
- : Pointer to the CatCache structure to be reset
- : Boolean flag indicating whether this is called from debug_discard_caches for testing purposes

## Dependencies
- Functions called/Symbols referenced:
  - CatCacheRemoveCList
  - CatCacheRemoveCTup
  - dlist_container
  - dlist_foreach_modify
- Called from (representative examples):
  - ResetCatalogCachesExt
  - CatalogCacheFlushCatalog

## Notes and Other Information
- This is a static function, only accessible within catcache.c
- The function is not optimized for efficiency when the cache is nearly empty, as it's not expected to be called frequently
- When debug_discard is true, in-progress builds are preserved to allow cache invalidation testing to proceed
- Dead entries are tracked for later cleanup when reference counts reach zero
- The function updates cache invalidation statistics when CATCACHE_STATS is enabled