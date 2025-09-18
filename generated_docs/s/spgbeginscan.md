# spgbeginscan

## Location
src/backend/access/spgist/spgscan.c: 304 - 379

## Overview
Initializes and begins a new SP-GiST index scan, setting up all necessary data structures and memory contexts for scanning operations.

## Definition
```c
IndexScanDesc spgbeginscan(Relation rel, int keysz, int orderbysz)
```

## Detailed Description
This function creates and initializes a new SP-GiST index scan descriptor. It allocates memory for the SpGistScanOpaque structure and sets up all necessary components including memory contexts for temporary storage and traversal values, tuple descriptors for index-only scans, and arrays for order-by operations.

The function also prepares function manager info for the inner and leaf consistent functions that will be used during scanning, and handles the setup of distance arrays for nearest-neighbor searches when order-by clauses are present.

## Parameters / Member Variables
- `rel`: The relation (index) being scanned
- `keysz`: Number of scan keys (search conditions)
- `orderbysz`: Number of order-by expressions for distance-ordered scans

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetIndexScan
  - palloc0/palloc
  - initSpGistState
  - AllocSetContextCreate
  - getSpGistTupleDesc
  - get_float8_infinity
  - fmgr_info_copy
  - index_getprocinfo
  - SPGIST_INNER_CONSISTENT_PROC (constant)
  - SPGIST_LEAF_CONSISTENT_PROC (constant)
  - ALLOCSET_DEFAULT_SIZES (constant)
- Called from:
  - spghandler (src/backend/access/spgist/spgutils.c:84)

## Dependencies
- Types used:
  - IndexScanDesc
  - SpGistScanOpaque
  - SpGistScanOpaqueData
  - ScanKey
  - Relation

## Notes and Other Information
- Creates two separate memory contexts: one for temporary search operations and another for traversal values
- Sets up tuple descriptor for potential index-only scans using getSpGistTupleDesc
- For distance-ordered scans, allocates and initializes arrays for zero distances and infinite distances
- Copies function manager information for inner and leaf consistent procedures from the index's operator class
- The returned IndexScanDesc needs to be used with spgrescan before actual scanning can begin
- Memory allocation uses CurrentMemoryContext as the parent for the created contexts