# systable_beginscan

## Location
[src/backend/access/index/genam.c:386-481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/genam.c#L386-L481)

## Overview
Initiates a flexible catalog scan that can use either heap scan or index scan depending on system state and availability of indexes.

## Definition

```c
SysScanDesc
systable_beginscan(Relation heapRelation,
				   Oid indexId,
				   bool indexOK,
				   Snapshot snapshot,
				   int nkeys, ScanKey key)
```
## Detailed Description
systable_beginscan is a high-level interface for scanning PostgreSQL system catalogs that provides automatic fallback between index and heap scans. The function intelligently chooses between using an index scan (when indexes are available and safe) or a heap scan (when indexes are unavailable, being rebuilt, or explicitly disabled). This capability is crucial for system catalog access during bootstrap, recovery, and reindexing operations when normal indexes may not be accessible.

The function handles snapshot management by automatically using catalog snapshots when none is provided, and manages the complex attribute number translation required when switching between heap and index scans. For index scans, it translates heap attribute numbers to index column numbers, ensuring that scan keys work correctly regardless of scan method.

## Parameters / Member Variables
- `heapRelation`: The catalog relation to scan, must be already opened and locked
- `indexId`: OID of the index to conditionally use for scanning
- `indexOK`: Boolean flag that can force heap scan even when index is available
- `snapshot`: Time qualification snapshot to use (NULL for automatic catalog snapshot)
- `nkeys`: Number of scan key conditions
- `key`: Array of scan key conditions for filtering
## Dependencies
- Functions called/Symbols referenced:
  - [index_open](../i/index_open.md) (open index relation)
  - [palloc](../p/palloc.md) (memory allocation)
  - [table_slot_create](../t/table_slot_create.md) (create tuple slot)
  - [GetCatalogSnapshot](../G/GetCatalogSnapshot.md) (get catalog snapshot)
  - [RegisterSnapshot](../R/RegisterSnapshot.md) (register snapshot)
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

## Simplified Source

```c
// Simplified version of systable_beginscan
SysScanDesc
systable_beginscan(Relation heapRelation,
                   Oid indexId,
                   bool indexOK,
                   Snapshot snapshot,
                   int nkeys, ScanKey key)
{
    SysScanDesc sysscan;
    Relation indexRelation = NULL;

    // Step 1: Determine scan method (index vs heap)
    if (indexOK && !IgnoreSystemIndexes && !ReindexIsProcessingIndex(indexId)) {
        indexRelation = index_open(indexId, AccessShareLock);
    }

    // Step 2: Initialize scan descriptor
    sysscan = (SysScanDesc) palloc(sizeof(SysScanDescData));
    sysscan->heap_rel = heapRelation;
    sysscan->irel = indexRelation;
    sysscan->slot = table_slot_create(heapRelation, NULL);

    // Step 3: Handle snapshot management
    if (snapshot == NULL) {
        // Auto-create catalog snapshot
        Oid relid = RelationGetRelid(heapRelation);
        snapshot = RegisterSnapshot(GetCatalogSnapshot(relid));
        sysscan->snapshot = snapshot;
    } else {
        // Use provided snapshot
        sysscan->snapshot = NULL;
    }

    // Step 4: Start appropriate scan type
    if (indexRelation) {
        // Index scan path: translate heap attribute numbers to index columns
        for (int i = 0; i < nkeys; i++) {
            for (int j = 0; j < IndexRelationGetNumberOfAttributes(indexRelation); j++) {
                if (key[i].sk_attno == indexRelation->rd_index->indkey.values[j]) {
                    key[i].sk_attno = j + 1;
                    break;
                }
            }
        }

        sysscan->iscan = index_beginscan(heapRelation, indexRelation, snapshot, nkeys, 0);
        index_rescan(sysscan->iscan, key, nkeys, NULL, 0);
        sysscan->scan = NULL;
    } else {
        // Heap scan path: disable sync scan for predictable performance
        sysscan->scan = table_beginscan_strat(heapRelation, snapshot, nkeys, key, true, false);
        sysscan->iscan = NULL;
    }

    // Step 5: Handle transaction state tracking
    if (TransactionIdIsValid(CheckXidAlive)) {
        bsysscan = true;
    }

    return sysscan;
}
```

Key simplifications made:
- Consolidated scan method decision logic into clear conditional flow
- Simplified attribute number translation loop with descriptive comments
- Removed detailed error handling for readability (kept essential logic)
- Added step-by-step comments explaining the major phases
- Abstracted complex snapshot management into high-level operations
- Maintained the core index vs heap scan decision logic
- Preserved essential transaction state tracking functionality