# SysScanDesc

## Location
[src/include/access/genam.h:91-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/genam.h#L91-L92)

## Overview
SysScanDesc is a pointer type to SysScanDescData structure that represents a system catalog scan descriptor, providing a unified interface for scanning PostgreSQL system tables using either heap scans or index scans.

## Definition


The actual structure definition (SysScanDescData) contains:


## Detailed Description
SysScanDesc serves as a unified descriptor for system catalog scans, abstracting whether the scan is performed using a heap scan or an index scan. This abstraction is crucial for PostgreSQL's catalog access patterns, where the system may choose to use either a sequential scan of the catalog table or an index scan based on the available indexes and search conditions.

The structure maintains references to both the catalog relation being scanned and optionally an index relation. Depending on the scan type, either the scan field (for heap scans) or iscan field (for index scans) will be valid, but not both simultaneously. The slot field provides a reusable tuple slot for retrieving results.

## Parameters / Member Variables
- : The system catalog relation being scanned
- : The index relation descriptor, NULL when performing a heap scan
- : Table scan descriptor, only valid for storage/heap scan operations
- : Index scan descriptor, only valid for index scan operations
- : Snapshot for visibility determination, automatically unregistered at scan end
- : Tuple table slot for storing and returning scan results

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDescData](SysScanDescData.md) (the actual structure definition)
  - [TableScanDescData](../T/TableScanDescData.md) (for heap scan operations)
  - [IndexScanDescData](../I/IndexScanDescData.md) (for index scan operations)
  - [SnapshotData](SnapshotData.md) (for snapshot management)
  - TupleTableSlot (for tuple storage)
- Called from (representative examples):
  - [systable_beginscan](../s/systable_beginscan.md)/systable_endscan (generic system table scan functions)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md)/systable_endscan_ordered (ordered system table scans)
  - [systable_getnext](../s/systable_getnext.md)/systable_getnext_ordered (retrieving next tuple from system scans)
  - [SearchCatCacheMiss](SearchCatCacheMiss.md) (catalog cache miss handling)
  - [RelationBuildTupleDesc](../R/RelationBuildTupleDesc.md)/RelationBuildRuleLock (relation cache building)
  - [GetDatabaseTuple](../G/GetDatabaseTuple.md)/GetDatabaseTupleByOid (database metadata access)

## Notes and Other Information
- This is defined in src/include/access/genam.h as a pointer typedef, with the actual structure in src/include/access/relscan.h
- Provides a unified interface for system catalog access, hiding the complexity of choosing between heap and index scans
- Essential for all system catalog operations including metadata lookups, dependency tracking, and schema information retrieval
- The snapshot field ensures proper visibility semantics for system catalog reads
- Used extensively throughout the catalog layer for operations like constraint checking, dependency analysis, and metadata queries
- The slot field enables efficient tuple handling and reduces memory allocation overhead
- Critical for maintaining ACID properties when accessing system catalogs
- Supports both ordered and unordered scan patterns depending on requirements
- The dual scan mechanism (heap vs index) allows PostgreSQL to optimize system catalog access patterns automatically