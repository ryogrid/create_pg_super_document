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
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [InvalidateSystemCachesExtended](InvalidateSystemCachesExtended.md)
- Called from (representative examples):
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md)
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md)
  - LogicalReplicationSlotHasPendingWal
  - [LogicalSlotAdvanceAndCheckSnapState](../L/LogicalSlotAdvanceAndCheckSnapState.md)

## Notes and Other Information
- This function is called when shared invalidation queue overflow is detected, indicating that some invalidation messages were lost
- The function performs a "nuclear option" cache flush since the system cannot determine exactly what needs to be invalidated
- [Relation](../R/Relation.md) descriptors with positive reference counts are automatically rebuilt after invalidation
- The function is heavily used in logical replication contexts where cache consistency is critical
- Performance impact is significant but necessary for correctness when message loss occurs