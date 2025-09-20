# InvalidateConstraintCacheCallBack

## Location
[src/backend/utils/adt/ri_triggers.c:2228-2268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L2228-L2268)

## Overview
Cache invalidation callback function for pg_constraint system catalog changes that intelligently invalidates constraint cache entries based on hash values.

## Definition

```c
static void
InvalidateConstraintCacheCallBack(Datum arg, int cacheid, uint32 hashvalue)
```
## Detailed Description
This function serves as a callback for PostgreSQL's cache invalidation system when pg_constraint catalog entries are modified. It implements smart invalidation rather than flushing all entries:

1. **Selective Invalidation**: Only invalidates entries whose hash values match the provided hashvalue
2. **Hierarchy Handling**: Invalidates both direct matches and child constraint entries to handle constraint inheritance
3. **Performance Optimization**: If the valid entries list exceeds 1000 items, it performs a full invalidation to avoid O(N²) behavior
4. **Safe Invalidation**: Marks entries as invalid rather than removing them, ensuring active references remain safe

The function is designed to handle high-traffic scenarios like pg_dump restores where many ALTER TABLE operations occur alongside foreign key usage.

## Parameters / Member Variables
- : Datum argument (unused in this callback)
- : Cache identifier for the invalidated cache
- : Hash value of the invalidated entry, or 0 for full cache reset

## Dependencies
- Functions called/Symbols referenced:
  - [dclist_count](../d/dclist_count.md)
  - dclist_foreach_modify
  - dclist_container
  - [dclist_delete_from](../d/dclist_delete_from.md)
  - Assert
- Called from (representative examples):
  - [ri_InitHashTables](../r/ri_InitHashTables.md) (registered as callback)

## Notes and Other Information
- Registered as a callback with the PostgreSQL cache invalidation system during ri_InitHashTables
- Uses doubly-linked list iteration with modification support for safe entry removal
- Implements a threshold-based optimization (1000 entries) to prevent performance degradation
- Handles both constraint OID hash values and root constraint hash values for proper inheritance support
- Marks entries invalid rather than deleting them to ensure thread safety with active references
- Part of PostgreSQL's sophisticated cache management system for referential integrity
- Located in src/backend/utils/adt/ri_triggers.c:2228-2268