# index_getbitmap

## Location
[src/backend/access/index/indexam.c:718-747](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/indexam.c#L718-L747)

## Overview
Retrieves all tuples at once from an index scan by adding the TIDs of all heap tuples satisfying the scan keys to a bitmap data structure.

## Definition
```c
int64 index_getbitmap(IndexScanDesc scan, TIDBitmap *bitmap)
```

## Detailed Description
The `index_getbitmap` function is a core indexing access method that performs bulk tuple retrieval from an index. Unlike sequential index scanning that returns tuples one by one, this function collects all matching tuple identifiers (TIDs) into a bitmap structure in a single operation. 

This approach is particularly useful for bitmap index scans where multiple indexes can be combined using bitmap operations (AND, OR) before accessing the actual heap tuples. The function is only safe to use with MVCC-based snapshots since there's no interlock between the index scan and eventual heap access - heap item slots could be replaced by newer tuples by the time they're accessed.

The function delegates the actual work to the access method's specific `amgetbitmap` procedure, allowing different index types (B-tree, GiST, GIN, etc.) to implement their own optimized bitmap collection strategies.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure containing the index scan state, including scan keys, index relation, and scan direction
- `bitmap`: TIDBitmap structure that will be populated with the TIDs of matching tuples

## Dependencies
- Functions called/Symbols referenced:
  - SCAN_CHECKS (macro for scan validation)
  - CHECK_SCAN_PROCEDURE (macro to verify amgetbitmap procedure exists)
  - pgstat_count_index_tuples (statistics collection)
  - amgetbitmap (access method specific bitmap collection procedure)
- Called from (representative examples):
  - [MultiExecBitmapIndexScan](../M/MultiExecBitmapIndexScan.md) (bitmap index scan executor)
  - IndexScanIsValid (index scan validation)

## Notes and Other Information
- Returns the number of matching tuples found, though this count might be approximate and should only be used for statistical purposes
- Sets `scan->kill_prior_tuple` to false as tuple killing is not applicable in bitmap scans
- The function includes statistics collection via `pgstat_count_index_tuples` to track index usage
- Only safe with MVCC snapshots due to lack of interlock between index scan and heap access
- Essential component of PostgreSQL's bitmap scan execution strategy for efficient multi-index queries