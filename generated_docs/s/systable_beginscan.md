# systable_beginscan

## Location
src/backend/access/index/genam.c: 386 - 481

## Overview
Initiates a flexible catalog scan that can use either heap scan or index scan depending on system state and availability of indexes.

## Definition


## Detailed Description
systable_beginscan is a high-level interface for scanning PostgreSQL system catalogs that provides automatic fallback between index and heap scans. The function intelligently chooses between using an index scan (when indexes are available and safe) or a heap scan (when indexes are unavailable, being rebuilt, or explicitly disabled). This capability is crucial for system catalog access during bootstrap, recovery, and reindexing operations when normal indexes may not be accessible.

The function handles snapshot management by automatically using catalog snapshots when none is provided, and manages the complex attribute number translation required when switching between heap and index scans. For index scans, it translates heap attribute numbers to index column numbers, ensuring that scan keys work correctly regardless of scan method.

## Parameters / Member Variables
- : The catalog relation to scan, must be already opened and locked
- : OID of the index to conditionally use for scanning
- : Boolean flag that can force heap scan even when index is available
- : Time qualification snapshot to use (NULL for automatic catalog snapshot)
- : Number of scan key conditions
- : Array of scan key conditions for filtering

## Dependencies
- Functions called/Symbols referenced:
  - [index_open](../i/index_open.md) (open index relation)
  - [palloc](../p/palloc.md) (memory allocation)
  - [table_slot_create](../t/table_slot_create.md) (create tuple slot)
  - [GetCatalogSnapshot](../G/GetCatalogSnapshot.md) (get catalog snapshot)
  - RegisterSnapshot (register snapshot)
  - IndexRelationGetNumberOfAttributes (get index column count)
  - [index_beginscan](../i/index_beginscan.md) (start index scan)
  - [index_rescan](../i/index_rescan.md) (initialize index scan with keys)
  - [table_beginscan_strat](../t/table_beginscan_strat.md) (start heap scan)
  - [ReindexIsProcessingIndex](../R/ReindexIsProcessingIndex.md) (check reindex state)
- Called from (representative examples):
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md) (OID generation)
  - [SearchCatCacheMiss](../S/SearchCatCacheMiss.md) (catalog cache operations)
  - [RelationBuildTupleDesc](../R/RelationBuildTupleDesc.md) (relation cache building)
  - [get_extension_oid](../g/get_extension_oid.md) (extension management)
  - [RemoveTriggerById](../R/RemoveTriggerById.md) (trigger management)

## Notes and Other Information
- Automatically falls back to heap scan when IgnoreSystemIndexes is set or during reindexing
- Translates heap attribute numbers to index column numbers when using index scan
- Disables synchronized scans for heap scans on catalogs to ensure predictable performance
- Manages snapshot lifecycle, creating catalog snapshots when none provided
- Sets special flags during CheckXidAlive transactions for transaction management
- Requires scan keys to be compatible with the specified index structure
- Essential for system catalog operations that must work during all phases of database operation