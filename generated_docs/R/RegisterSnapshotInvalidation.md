# RegisterSnapshotInvalidation

## Location
[src/backend/utils/cache/inval.c:601-611](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L601-L611)

## Overview
Registers an invalidation event for MVCC scans against a given catalog, specifically designed for catalogs that don't have catcaches.

## Definition
```c
static void RegisterSnapshotInvalidation(Oid dbId, Oid relId)
```

## Detailed Description
RegisterSnapshotInvalidation is a static function that handles snapshot invalidation for catalogs that are accessed via MVCC scans rather than through the catalog cache system. This function is specifically needed for catalogs that don't have catcaches, as they require a different invalidation mechanism.

The function works by adding a snapshot invalidation message to the current command's invalidation message queue. When processed, these messages will cause MVCC snapshots that might be scanning the specified catalog to be invalidated, ensuring that subsequent scans will see current data.

This is part of PostgreSQL's multi-layered cache invalidation system, complementing catalog cache and relation cache invalidation for cases where direct catalog table scanning is used.

## Parameters / Member Variables
- `dbId`: Database OID where the catalog resides
- `relId`: OID of the catalog relation being invalidated

## Dependencies
- Functions called/Symbols referenced:
  - [AddSnapshotInvalidationMessage](../A/AddSnapshotInvalidationMessage.md)
- Called from (representative examples):
  - [CacheInvalidateHeapTuple](../C/CacheInvalidateHeapTuple.md)

## Notes and Other Information
- This is a static function internal to the invalidation system
- Specifically designed for catalogs without catcaches that are accessed via MVCC scans
- Part of the comprehensive invalidation system that covers catcache, relcache, and snapshot invalidation
- Less commonly used than catalog and relcache invalidation since most system catalogs have catcaches
- Essential for ensuring consistency when catalog tables are modified but don't participate in the catcache system

## Simplified Source

```c
static void RegisterSnapshotInvalidation(Oid dbId, Oid relId)
{
    // Add snapshot invalidation message to current command's message group
    AddSnapshotInvalidationMessage(&transInvalInfo->CurrentCmdInvalidMsgs, dbId, relId);
}
```