# systable_beginscan_ordered

## Location
src/backend/access/index/genam.c: 645 - 719

## Overview
systable_beginscan_ordered is a specialized system catalog scanning function that guarantees to return matching tuples in index order, providing ordered access to catalog data.

## Definition
```c
SysScanDesc systable_beginscan_ordered(Relation heapRelation, Relation indexRelation, Snapshot snapshot, int nkeys, ScanKey key)
```

## Detailed Description
This function provides ordered catalog scanning capabilities by wrapping index-based scan operations. Unlike systable_beginscan, it requires the caller to provide an already opened and locked index relation and guarantees that multiple matching tuples will be returned in index order. This is particularly useful for operations that need predictable ordering of catalog entries.

The function performs several setup operations: it validates that the index is not being reindexed (throwing an error if so), creates a scan descriptor with the provided relations, sets up tuple slots and snapshots, converts scan key attribute numbers from heap column numbers to index column numbers, and initializes the index scan with the converted keys.

The function includes special handling for system index access policies (IgnoreSystemIndexes) and transaction monitoring for logical replication (CheckXidAlive/bsysscan flags). Currently, it only supports index-based scans and does not provide heap scan fallback with sorting.

## Parameters / Member Variables
- : The heap relation (table) to scan
- : The index relation to use for ordered scanning (must be pre-opened by caller)
- : The snapshot to use for MVCC visibility (NULL for catalog snapshot)
- : Number of scan keys in the key array
- : Array of ScanKey structures defining the scan conditions

## Dependencies
- Functions called/Symbols referenced:
  - [ReindexIsProcessingIndex](../R/ReindexIsProcessingIndex.md) (reindex validation)
  - RelationGetRelid (relation OID retrieval)
  - RelationGetRelationName (relation name retrieval)
  - [palloc](../p/palloc.md) (memory allocation)
  - [table_slot_create](../t/table_slot_create.md) (tuple slot creation)
  - [GetCatalogSnapshot](../G/GetCatalogSnapshot.md) (snapshot acquisition)
  - RegisterSnapshot (snapshot registration)
  - IndexRelationGetNumberOfAttributes (index introspection)
  - [index_beginscan](../i/index_beginscan.md) (index scan initialization)
  - [index_rescan](../i/index_rescan.md) (index scan parameter setup)
  - TransactionIdIsValid (transaction validation)
- Called from (representative examples):
  - [toast_delete_datum](../t/toast_delete_datum.md)
  - [heap_fetch_toast_slice](../h/heap_fetch_toast_slice.md)
  - [inv_getsize](../i/inv_getsize.md) (large object operations)
  - [inv_read](../i/inv_read.md), inv_write, inv_truncate (large object I/O)
  - [enum_endpoint](../e/enum_endpoint.md), enum_range_internal (enum type operations)
  - [BuildEventTriggerCache](../B/BuildEventTriggerCache.md)
  - [lookup_ts_config_cache](../l/lookup_ts_config_cache.md)

## Notes and Other Information
- Requires caller to open and lock the index relation beforehand, unlike regular systable_beginscan
- Does not support non-index-based scans (no heap scan with sort fallback)
- Provides the foundation for potential future support of lossy operators in catalog scans
- Includes error handling for reindex conflicts and warnings for IgnoreSystemIndexes violations
- Converts scan key attribute numbers from heap column positions to index column positions
- Part of PostgreSQL's specialized catalog access infrastructure for operations requiring ordered results
- The ordered guarantee makes it suitable for operations like enum value processing and large object chunk access