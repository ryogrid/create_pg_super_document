# index_beginscan_internal

## Location
src/backend/access/index/indexam.c: 310 - 351

## Overview
The  function is the common internal implementation for all index scan initialization variants, handling the core setup logic shared across different scan types.

## Definition


## Detailed Description
This static function serves as the unified backend for all index scan initialization operations in PostgreSQL. It performs essential setup tasks including relation validation, access method verification, predicate locking (when applicable), reference counting management, and delegation to the access method-specific scan initialization routine. The function handles both regular and parallel scan scenarios through its parameters. It ensures that proper locks and reference counts are maintained throughout the scan lifecycle and initializes the scan descriptor with parallel scan information and temporary snapshot flags as needed.

## Parameters / Member Variables
- : The index relation to be scanned
- : Number of scan keys (search conditions) for the scan
- : Number of ordering specifications for the scan  
- : The snapshot for visibility checking (used for predicate locking)
- : Parallel index scan descriptor for parallel query execution (can be NULL for non-parallel scans)
- : Boolean flag indicating whether the snapshot is temporary

## Dependencies
- Functions called/Symbols referenced:
  - RELATION_CHECKS (macro for relation validation)
  - CHECK_REL_PROCEDURE (macro to verify ambeginscan procedure exists)
  - [PredicateLockRelation](../P/PredicateLockRelation.md) (predicate locking for serializable isolation)
  - [RelationIncrementReferenceCount](../R/RelationIncrementReferenceCount.md) (reference count management)
  - [ParallelIndexScanDesc](../P/ParallelIndexScanDesc.md) (parallel scan descriptor type)
  - [IndexScanDesc](../I/IndexScanDesc.md) (return type structure)
- Called from (representative examples):
  - [index_beginscan](index_beginscan.md) (src/backend/access/index/indexam.c:265)
  - [index_beginscan_bitmap](index_beginscan_bitmap.md) (src/backend/access/index/indexam.c:295)
  - [index_beginscan_parallel](index_beginscan_parallel.md) (src/backend/access/index/indexam.c:550)

## Notes and Other Information
- This is a static function providing shared implementation for public index scan functions
- Handles predicate locking for serializable transactions when access method doesn't provide predicate locks
- Maintains reference counting to prevent index relation from being dropped during scan
- Supports both parallel and non-parallel scan initialization through optional parameters
- Delegates actual scan setup to access method-specific ambeginscan function
- Located in src/backend/access/index/indexam.c:310-351