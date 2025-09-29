# InvalidateSystemCaches

## Location
[src/backend/utils/cache/inval.c:793-806](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L793-L806)

## Overview
Invalidates all system catalog caches, relation descriptors, and storage manager cache entries when shared invalidation queue overflow is detected.

## Definition

```c
void
InvalidateSystemCaches(void)
```
## Detailed Description
InvalidateSystemCaches is a comprehensive cache invalidation function that performs a complete flush of all PostgreSQL caches when the system detects that shared invalidation messages have been lost due to queue overflow. This is a drastic but necessary measure to ensure cache consistency across the system.

The function is a simple wrapper that calls InvalidateSystemCachesExtended(false), which performs the actual work of:
1. Invalidating the catalog snapshot
2. Resetting all catalog caches 
3. Invalidating all relation cache entries (which also handles storage manager and relation map caches)
4. Executing all registered syscache and relcache callbacks

This comprehensive invalidation ensures that no stale cache entries remain when the system cannot determine exactly which caches need to be invalidated due to lost messages.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [InvalidateSystemCachesExtended](InvalidateSystemCachesExtended.md)
- Called from (representative examples):
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md)
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md)
  - [LogicalReplicationSlotHasPendingWal](../L/LogicalReplicationSlotHasPendingWal.md)
  - [LogicalSlotAdvanceAndCheckSnapState](../L/LogicalSlotAdvanceAndCheckSnapState.md)

## Notes and Other Information
- This function is called when shared invalidation queue overflow is detected, indicating that some invalidation messages were lost
- The function performs a "nuclear option" cache flush since the system cannot determine exactly what needs to be invalidated
- [Relation](../R/Relation.md) descriptors with positive reference counts are automatically rebuilt after invalidation
- The function is heavily used in logical replication contexts where cache consistency is critical
- Performance impact is significant but necessary for correctness when message loss occurs

## Simplified Source

```c
// Simplified version of InvalidateSystemCaches
void InvalidateSystemCaches(void) {
    // Delegate to extended version with debug_discard = false
    InvalidateSystemCachesExtended(false);
}

// Core implementation (InvalidateSystemCachesExtended)
void InvalidateSystemCachesExtended(bool debug_discard) {
    // Step 1: Invalidate catalog snapshot
    InvalidateCatalogSnapshot();

    // Step 2: Reset all catalog caches
    ResetCatalogCachesExt(debug_discard);

    // Step 3: Invalidate relation cache (includes smgr and relmap caches)
    RelationCacheInvalidate(debug_discard);

    // Step 4: Execute all registered syscache callbacks
    for (int i = 0; i < syscache_callback_count; i++) {
        syscache_callback_list[i].function(
            syscache_callback_list[i].arg,
            syscache_callback_list[i].id,
            0
        );
    }

    // Step 5: Execute all registered relcache callbacks
    for (int i = 0; i < relcache_callback_count; i++) {
        relcache_callback_list[i].function(
            relcache_callback_list[i].arg,
            InvalidOid
        );
    }
}
```

Key simplifications made:
- Combined both functions to show complete invalidation flow
- Simplified callback loop structure for clarity
- Added step-by-step comments explaining the invalidation sequence
- Removed complex struct pointer arithmetic, using array notation instead
- Focused on the logical flow rather than low-level implementation details