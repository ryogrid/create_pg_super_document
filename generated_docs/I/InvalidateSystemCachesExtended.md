# InvalidateSystemCachesExtended

## Location
[src/backend/utils/cache/inval.c:675-705](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L675-L705)

## Overview
Performs comprehensive invalidation of all PostgreSQL system caches, including catalog snapshots, catalog caches, relation caches, and executes registered invalidation callbacks.

## Definition
```c
void InvalidateSystemCachesExtended(bool debug_discard)
```

## Detailed Description
InvalidateSystemCachesExtended is a public function that performs a complete invalidation of PostgreSQL's caching system. This is a heavyweight operation that clears all cached data and forces the system to reload information from disk on subsequent accesses.

The function operates in several phases:
1. **Snapshot Invalidation**: Calls InvalidateCatalogSnapshot() to invalidate the current catalog snapshot
2. **Catalog Cache Reset**: Calls ResetCatalogCachesExt() to clear all catalog cache entries
3. **Relation Cache Invalidation**: Calls RelationCacheInvalidate() which also handles storage manager and relation mapping caches
4. **Callback Execution**: Executes all registered syscache and relcache callbacks to notify external components

The debug_discard parameter controls whether cache entries are truly discarded or just marked invalid, which is useful for debugging cache-related issues.

This function is typically used during major system events like cache poisoning recovery, debugging scenarios, or when fundamental system metadata has changed in ways that require complete cache reconstruction.

## Parameters / Member Variables
- `debug_discard`: Boolean flag controlling whether to completely discard cache entries (true) or just mark them invalid (false)

## Dependencies
- Functions called/Symbols referenced:
  - [InvalidateCatalogSnapshot](InvalidateCatalogSnapshot.md)
  - [ResetCatalogCachesExt](../R/ResetCatalogCachesExt.md)
  - [RelationCacheInvalidate](../R/RelationCacheInvalidate.md)
  - [SYSCACHECALLBACK](../S/SYSCACHECALLBACK.md) (struct type)
  - [RELCACHECALLBACK](../R/RELCACHECALLBACK.md) (struct type)
- Called from (representative examples):
  - [InvalidateSystemCaches](InvalidateSystemCaches.md)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - INVAL_H (header reference)

## Notes and Other Information
- This is a public function available to external callers
- Very expensive operation that should be used sparingly
- Executes callbacks to allow extensions and other components to respond to cache invalidation
- The debug_discard parameter is primarily used for testing and debugging cache behavior
- Comprehensive invalidation ensures no stale cache entries remain after execution
- Part of PostgreSQL's robust cache consistency infrastructure
- Used in scenarios where selective invalidation is insufficient or when cache corruption is suspected

## Simplified Source

```c
// Simplified version of InvalidateSystemCachesExtended
void InvalidateSystemCachesExtended(bool debug_discard) {
    // Step 1: Invalidate catalog snapshot to ensure fresh metadata views
    InvalidateCatalogSnapshot();

    // Step 2: Reset all catalog caches with debug discard option
    ResetCatalogCachesExt(debug_discard);

    // Step 3: Invalidate relation caches (also handles storage manager and relation mapping)
    RelationCacheInvalidate(debug_discard);

    // Step 4: Execute all registered system cache invalidation callbacks
    for (int i = 0; i < syscache_callback_count; i++) {
        struct SYSCACHECALLBACK *callback = syscache_callback_list + i;
        callback->function(callback->arg, callback->id, 0);
    }

    // Step 5: Execute all registered relation cache invalidation callbacks
    for (int i = 0; i < relcache_callback_count; i++) {
        struct RELCACHECALLBACK *callback = relcache_callback_list + i;
        callback->function(callback->arg, InvalidOid);
    }
}
```

Key simplifications made:
- Added descriptive comments explaining each phase of invalidation
- Used more descriptive variable names (`callback` instead of `ccitem`)
- Consolidated callback loop logic for clarity
- Emphasized the sequential nature of the invalidation process
- Removed unnecessary variable declarations while preserving logic flow