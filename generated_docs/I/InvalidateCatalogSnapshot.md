# InvalidateCatalogSnapshot

## Location
src/backend/utils/time/snapmgr.c: 422 - 442

## Overview
Marks the current catalog snapshot as invalid and removes it from the registered snapshots, forcing creation of a fresh snapshot on next catalog access.

## Definition
```c
void InvalidateCatalogSnapshot(void)
```

## Detailed Description
InvalidateCatalogSnapshot provides a mechanism to force refresh of the cached catalog snapshot when catalog data may have changed. The function performs cleanup by removing the CatalogSnapshot from the RegisteredSnapshots pairing heap (reversing the manual registration done in GetNonHistoricCatalogSnapshot), setting the global CatalogSnapshot pointer to NULL, and calling SnapshotResetXmin() to update transaction visibility calculations. 

The function uses a coarse-grained invalidation approach - any catalog change invalidates the entire snapshot rather than tracking fine-grained per-relation invalidations, as the performance benefit of such tracking has not been demonstrated.

## Parameters / Member Variables
- None (operates on global CatalogSnapshot state)

## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_remove
  - SnapshotResetXmin
- Called from (representative examples):
  - heap_inplace_lock
  - CopyFrom
  - InvalidateSystemCachesExtended
  - LocalExecuteInvalidationMessage
  - GetTransactionSnapshot
  - GetNonHistoricCatalogSnapshot
  - InvalidateCatalogSnapshotConditionally
  - AtEOXact_Snapshot

## Notes and Other Information
- Uses coarse-grained invalidation strategy for simplicity
- Essential for maintaining catalog consistency after DDL operations
- Must be paired with GetNonHistoricCatalogSnapshot registration logic
- Called extensively by cache invalidation system
- Located in src/backend/utils/time/snapmgr.c:422-442
- Global CatalogSnapshot state management