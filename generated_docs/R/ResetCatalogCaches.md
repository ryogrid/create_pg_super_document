# ResetCatalogCaches

## Location
src/backend/utils/cache/catcache.c: 798 - 803

## Overview
Resets all catalog caches when a shared cache invalidation event forces it, serving as a simple wrapper function.

## Definition
```c
void ResetCatalogCaches(void)
```

## Detailed Description
ResetCatalogCaches is a public function that provides a simple interface for resetting all catalog caches. It is typically called in response to shared cache invalidation events that require all caches to be cleared. The function is implemented as a thin wrapper around ResetCatalogCachesExt, passing false for the debug_discard parameter to indicate this is a normal (non-debug) cache reset operation.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [ResetCatalogCachesExt](ResetCatalogCachesExt.md)
- Called from (representative examples):
  - Referenced in CatCacheHeader

## Notes and Other Information
- This is a public function accessible from other modules
- Serves as the standard entry point for cache invalidation in normal operations
- Always performs a non-debug reset (debug_discard = false)
- Part of PostgreSQL's cache invalidation system for maintaining cache consistency across processes