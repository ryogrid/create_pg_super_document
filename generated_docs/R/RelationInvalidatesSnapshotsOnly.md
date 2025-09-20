# RelationInvalidatesSnapshotsOnly

## Location
[src/backend/utils/cache/syscache.c:723-745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/syscache.c#L723-L745)

## Overview
Determines whether a given relation sends snapshot invalidation messages instead of catalog cache invalidation messages.

## Definition

```c
bool
RelationInvalidatesSnapshotsOnly(Oid relid)
```
## Detailed Description
RelationInvalidatesSnapshotsOnly identifies relations that do not have system caches but need to send snapshot invalidation messages for consistency. This mechanism benefits GetCatalogSnapshot() by allowing it to reuse existing MVCC snapshots when scanning these catalogs, provided no invalidation has occurred. The function returns true for specific system relations that use snapshot invalidation rather than catalog cache invalidation. Relations with syscaches should not be included in this list as their catcache invalidation messages also flush snapshots.

## Parameters / Member Variables
- `relid`: Object identifier (Oid) of the relation to check

## Dependencies
- Functions called/Symbols referenced:
  - None (pure switch statement logic)
- Called from (representative examples):
  - [CacheInvalidateHeapTuple](../C/CacheInvalidateHeapTuple.md)
  - [InitCatalogCache](../I/InitCatalogCache.md)  
  - [GetNonHistoricCatalogSnapshot](../G/GetNonHistoricCatalogSnapshot.md)

## Notes and Other Information
- Currently handles 7 specific system relations: DbRoleSettingRelationId, DependRelationId, SharedDependRelationId, DescriptionRelationId, SharedDescriptionRelationId, SecLabelRelationId, and SharedSecLabelRelationId
- Relations that gain syscaches should be removed from this list to avoid conflicts
- Part of PostgreSQL's invalidation mechanism that optimizes snapshot reuse for certain system catalogs
- Used by the snapshot management system to determine invalidation behavior
- Located in src/backend/utils/cache/syscache.c:723-745