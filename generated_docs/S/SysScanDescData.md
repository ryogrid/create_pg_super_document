# SysScanDescData

## Location
src/include/access/relscan.h: 181 - 189

## Overview
SysScanDescData is a structure that encapsulates the state information for scanning system catalog tables, supporting both heap scans and index scans with automatic fallback mechanisms when system indexes are unavailable.

## Definition


## Detailed Description
This structure provides a unified interface for scanning PostgreSQL system catalogs (pg_class, pg_attribute, pg_proc, etc.) with intelligent scan method selection. The structure supports both index-based and heap-based scanning modes, automatically choosing the most appropriate method based on system conditions.

The design allows PostgreSQL to maintain system catalog access even when system indexes are corrupted, being rebuilt, or explicitly disabled via IgnoreSystemIndexes. When an index scan is not possible or advisable, the system transparently falls back to sequential heap scanning while maintaining the same API for callers.

This dual-mode capability is crucial for system recovery scenarios and bootstrapping operations where the normal index infrastructure may not be available. The structure maintains separate scan descriptors for each mode (scan for heap, iscan for index) with only one being active at any given time.

## Parameters / Member Variables
- : The system catalog relation being scanned (e.g., pg_class, pg_attribute)
- : The index relation used for index scanning; set to NULL when performing heap scans
- : Table scan descriptor used only when performing sequential heap scans; NULL during index scans
- : Index scan descriptor used only when performing index scans; NULL during heap scans
- : Transaction snapshot for determining tuple visibility; automatically registered/unregistered as needed
- : Tuple table slot for holding retrieved tuples during the scan operation

## Dependencies
- Functions called/Symbols referenced:
  - TableScanDescData (for heap scans)
  - IndexScanDescData (for index scans)
  - SnapshotData (for transaction visibility)
- Called from (representative examples):
  - systable_beginscan
  - systable_beginscan_ordered
  - SysScanDesc (typedef pointer)

## Notes and Other Information
- This structure is central to PostgreSQL's catalog access mechanisms and is used extensively throughout the system for metadata operations
- The structure automatically handles attribute number mapping when switching between heap and index scan modes (heap uses actual column numbers, indexes use index column positions)
- Supports both unordered scanning (systable_beginscan) and ordered scanning (systable_beginscan_ordered) with guaranteed index-order results
- Includes built-in protection against synchronized scans on catalogs to ensure predictable performance characteristics
- The snapshot management ensures proper MVCC behavior during catalog scans, with automatic cleanup when scans complete
- Critical for system bootstrap, recovery operations, and normal catalog lookups throughout PostgreSQL operation
- Part of a complete API including systable_getnext(), systable_endscan(), and related functions for complete scan lifecycle management