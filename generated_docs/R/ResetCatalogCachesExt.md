# ResetCatalogCachesExt

## Location
src/backend/utils/cache/catcache.c: 804 - 833

## Overview
Extended version of catalog cache reset that iterates through all catalog caches and resets each one, with optional debug mode support.

## Definition
```c
void ResetCatalogCachesExt(bool debug_discard)
```

## Detailed Description
ResetCatalogCachesExt is the core implementation function for resetting all catalog caches in the system. It iterates through the global list of catalog caches maintained in CacheHdr->ch_caches and calls ResetCatalogCache for each individual cache. The function provides debug logging to track when cache resets begin and end, which is useful for diagnosing cache-related issues.

The function supports both normal cache invalidation operations and debug mode operations where the goal is to test cache invalidation mechanisms rather than perform actual correctness-driven cache clearing.

## Parameters / Member Variables
- `debug_discard`: Boolean flag indicating whether this is a debug discard operation for testing cache invalidation mechanisms

## Dependencies
- Functions called/Symbols referenced:
  - ResetCatalogCache
  - slist_foreach
  - slist_container
  - CACHE_elog
  - DEBUG2
- Called from (representative examples):
  - ResetCatalogCaches
  - InvalidateSystemCachesExtended
  - Referenced in CatCacheHeader

## Notes and Other Information
- This is a public function that serves as the main entry point for system-wide cache resets
- Uses the global CacheHdr structure to access the list of all catalog caches
- Provides debug logging at DEBUG2 level to help with cache-related debugging
- The debug_discard parameter controls whether in-progress cache builds are preserved during reset
- Critical component of PostgreSQL's cache consistency and invalidation system