# InvalidateCatalogSnapshot

## Location
[src/backend/utils/time/snapmgr.c:422-442](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L422-L442)

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


## Dependencies
- Functions called/Symbols referenced:
  - [pairingheap_remove](../p/pairingheap_remove.md)
  - [SnapshotResetXmin](../S/SnapshotResetXmin.md)
- Called from (representative examples):
  - [heap_inplace_lock](../h/heap_inplace_lock.md)
  - [CopyFrom](../C/CopyFrom.md)
  - [InvalidateSystemCachesExtended](InvalidateSystemCachesExtended.md)
  - [LocalExecuteInvalidationMessage](../L/LocalExecuteInvalidationMessage.md)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)
  - [GetNonHistoricCatalogSnapshot](../G/GetNonHistoricCatalogSnapshot.md)
  - [InvalidateCatalogSnapshotConditionally](InvalidateCatalogSnapshotConditionally.md)
  - [AtEOXact_Snapshot](../A/AtEOXact_Snapshot.md)

## Notes and Other Information
- Uses coarse-grained invalidation strategy for simplicity
- Essential for maintaining catalog consistency after DDL operations
- Must be paired with GetNonHistoricCatalogSnapshot registration logic
- Called extensively by cache invalidation system
- Located in src/backend/utils/time/snapmgr.c:422-442
- Global CatalogSnapshot state management