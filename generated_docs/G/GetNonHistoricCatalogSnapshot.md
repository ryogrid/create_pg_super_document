# GetNonHistoricCatalogSnapshot

## Location
src/backend/utils/time/snapmgr.c: 374 - 421

## Overview
Provides a current (non-historic) snapshot for system catalog scans with intelligent caching and invalidation logic based on relation characteristics.

## Definition
```c
Snapshot GetNonHistoricCatalogSnapshot(Oid relid)
```

## Detailed Description
GetNonHistoricCatalogSnapshot implements sophisticated snapshot management for system catalog access. It maintains a cached CatalogSnapshot but intelligently invalidates it based on the target relation's characteristics. For relations that lack both syscache and snapshot-only invalidations, the function refreshes the snapshot on every call to ensure data consistency. When creating a new snapshot, it manually adds it to the RegisteredSnapshots pairing heap for proper PGPROC->xmin accounting, avoiding the overhead of RegisterSnapshot while maintaining proper snapshot lifecycle management.

The function's logic ensures that catalog scans always see sufficiently recent data while optimizing performance through selective caching.

## Parameters / Member Variables
- `relid`: OID of the system catalog relation being scanned

## Dependencies
- Functions called/Symbols referenced:
  - RelationInvalidatesSnapshotsOnly
  - RelationHasSysCache
  - InvalidateCatalogSnapshot
  - GetSnapshotData
  - pairingheap_add
- Called from (representative examples):
  - ScanPgRelation
  - GetCatalogSnapshot

## Notes and Other Information
- Uses CatalogSnapshot global variable for caching
- Manually manages RegisteredSnapshots heap to avoid resource owner dependencies
- Critical for relations without syscache/snapshot invalidation mechanisms
- Invalidation logic prevents stale catalog data visibility
- Must be paired with proper InvalidateCatalogSnapshot calls
- Located in src/backend/utils/time/snapmgr.c:374-421