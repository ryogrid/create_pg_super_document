# SysScanDescData

## Location
[src/include/access/relscan.h:181-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/relscan.h#L181-L189)

## Overview
SysScanDescData is a structure that encapsulates the state information for scanning system catalog tables, supporting both heap scans and index scans with automatic fallback mechanisms when system indexes are unavailable.

## Definition

```c
typedef struct SysScanDescData
{
	Relation	heap_rel;		/* catalog being scanned */
	Relation	irel;			/* NULL if doing heap scan */
	struct TableScanDescData *scan; /* only valid in storage-scan case */
	struct IndexScanDescData *iscan;	/* only valid in index-scan case */
	struct SnapshotData *snapshot;	/* snapshot to unregister at end of scan */
	struct TupleTableSlot *slot;
}			SysScanDescData;
```
## Detailed Description
This structure provides a unified interface for scanning PostgreSQL system catalogs (pg_class, pg_attribute, pg_proc, etc.) with intelligent scan method selection. The structure supports both index-based and heap-based scanning modes, automatically choosing the most appropriate method based on system conditions.

The design allows PostgreSQL to maintain system catalog access even when system indexes are corrupted, being rebuilt, or explicitly disabled via IgnoreSystemIndexes. When an index scan is not possible or advisable, the system transparently falls back to sequential heap scanning while maintaining the same API for callers.

This dual-mode capability is crucial for system recovery scenarios and bootstrapping operations where the normal index infrastructure may not be available. The structure maintains separate scan descriptors for each mode (scan for heap, iscan for index) with only one being active at any given time.

## Parameters / Member Variables
- `heap_rel`: The system catalog relation being scanned (e.g., pg_class, pg_attribute)
- `irel`: The index relation used for index scanning; set to NULL when performing heap scans
- `*scan`: Table scan descriptor used only when performing sequential heap scans; NULL during index scans
- `*iscan`: Index scan descriptor used only when performing index scans; NULL during heap scans
- `*snapshot`: Transaction snapshot for determining tuple visibility; automatically registered/unregistered as needed
- `*slot`: Tuple table slot for holding retrieved tuples during the scan operation
## Dependencies
- Functions called/Symbols referenced:
  - [TableScanDescData](../T/TableScanDescData.md) (for heap scans)
  - [IndexScanDescData](../I/IndexScanDescData.md) (for index scans)
  - [SnapshotData](SnapshotData.md) (for transaction visibility)
- Called from (representative examples):
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md)
  - [SysScanDesc](SysScanDesc.md) (typedef pointer)

## Notes and Other Information
- This structure is central to PostgreSQL's catalog access mechanisms and is used extensively throughout the system for metadata operations
- The structure automatically handles attribute number mapping when switching between heap and index scan modes (heap uses actual column numbers, indexes use index column positions)
- Supports both unordered scanning (systable_beginscan) and ordered scanning (systable_beginscan_ordered) with guaranteed index-order results
- Includes built-in protection against synchronized scans on catalogs to ensure predictable performance characteristics
- The snapshot management ensures proper MVCC behavior during catalog scans, with automatic cleanup when scans complete
- Critical for system bootstrap, recovery operations, and normal catalog lookups throughout PostgreSQL operation
- Part of a complete API including systable_getnext(), systable_endscan(), and related functions for complete scan lifecycle management