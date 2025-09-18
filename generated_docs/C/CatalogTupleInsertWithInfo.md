# CatalogTupleInsertWithInfo

## Location
src/backend/catalog/indexing.c: 256 - 272

## Overview
CatalogTupleInsertWithInfo inserts a tuple into a system catalog relation using caller-supplied index information, optimizing performance for bulk operations by amortizing index management overhead across multiple insertions.

## Definition
```c
void CatalogTupleInsertWithInfo(Relation heapRel, HeapTuple tup, CatalogIndexState indstate)
```

## Detailed Description
CatalogTupleInsertWithInfo is an optimized version of catalog tuple insertion that accepts pre-opened index state from the caller. This design pattern is intended to improve performance when performing multiple catalog insertions by avoiding the overhead of repeatedly opening and closing catalog indexes. The function performs constraint checking, inserts the tuple into the heap relation, and updates all associated indexes.

The function is designed for scenarios where it's important to amortize the cost of CatalogOpenIndexes/CatalogCloseIndexes operations across multiple insertions. While PostgreSQL may cache CatalogIndexState data in the future (possibly in the relcache), this optimization is currently the caller's responsibility.

## Parameters / Member Variables
- `heapRel`: The system catalog relation where the tuple will be inserted
- `tup`: The HeapTuple to be inserted into the catalog
- `indstate`: Pre-opened CatalogIndexState containing information about all indexes on the relation

## Dependencies
- Functions called/Symbols referenced:
  - [CatalogTupleCheckConstraints](CatalogTupleCheckConstraints.md)
  - [simple_heap_insert](../s/simple_heap_insert.md)
  - [CatalogIndexInsert](CatalogIndexInsert.md)
  - TU_All
- Called from (representative examples):
  - [CopyStatistics](CopyStatistics.md)
  - [update_attstats](../u/update_attstats.md)
  - [inv_write](../i/inv_write.md)
  - [inv_truncate](../i/inv_truncate.md)

## Notes and Other Information
- This function is part of PostgreSQL's catalog management infrastructure, specifically designed for performance optimization in bulk operations
- The caller is responsible for managing the CatalogIndexState lifecycle (opening and closing indexes)
- All catalog constraints are checked before insertion to maintain system catalog integrity
- The TU_All flag indicates that all indexes should be updated during the insertion operation
- This function is commonly used in statistics updates and large object operations where multiple catalog modifications occur