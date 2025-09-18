# _bt_first

## Location
[src/backend/access/nbtree/nbtsearch.c:876-1495](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsearch.c#L876-L1495)

## Overview
Finds the first item in a B-tree scan, positioning the scan at the appropriate starting point based on the scan keys and direction, handling complex scenarios like parallel scans and array keys.

## Definition
```c
bool _bt_first(IndexScanDesc scan, ScanDirection dir)
```

## Detailed Description
This function is the entry point for initializing a B-tree index scan. It determines where to start the scan based on the provided scan keys and direction, then positions the scan at the appropriate tuple. The function handles multiple complex scenarios including parallel scans, array keys, boundary key selection, and various scan strategies (equality, range queries, etc.).

The function processes scan keys to build an insertion-type scan key for tree traversal, handles special cases like NULL values and row comparisons, and sets up the scan state for subsequent _bt_next() calls. It supports both forward and backward scans and manages parallel scan coordination when multiple workers are involved.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing the scan state and parameters
- `dir`: ScanDirection indicating forward or backward scan direction

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_preprocess_keys](_bt_preprocess_keys.md)
  - [_bt_parallel_seize](_bt_parallel_seize.md)
  - [_bt_parallel_readpage](_bt_parallel_readpage.md)
  - [_bt_start_array_keys](_bt_start_array_keys.md)
  - [_bt_endpoint](_bt_endpoint.md)
  - [_bt_search](_bt_search.md)
  - [_bt_binsrch](_bt_binsrch.md)
  - [_bt_readpage](_bt_readpage.md)
  - [_bt_steppage](_bt_steppage.md)
  - [_bt_metaversion](_bt_metaversion.md)
  - [_bt_initialize_more_data](_bt_initialize_more_data.md)
  - [_bt_unlockbuf](_bt_unlockbuf.md)
  - [_bt_drop_lock_and_maybe_pin](_bt_drop_lock_and_maybe_pin.md)
  - [_bt_freestack](_bt_freestack.md)
  - [_bt_parallel_done](_bt_parallel_done.md)
  - pgstat_count_index_scan
  - [PredicateLockRelation](../P/PredicateLockRelation.md)
  - [PredicateLockPage](../P/PredicateLockPage.md)
- Called from:
  - [btgettuple](btgettuple.md)
  - [btgetbitmap](btgetbitmap.md)

## Notes and Other Information
- Returns true if a matching tuple is found, false if no matches exist
- Handles parallel scan coordination and load balancing across multiple workers
- Processes complex scan key combinations including row comparisons and array keys
- Implements boundary key selection logic for optimal scan starting positions
- Sets up scan positioning for various strategies (=, <, <=, >, >=)
- Manages buffer locking and predicate locking for proper concurrency control
- Critical for B-tree scan performance as it determines the optimal starting point
- The function can handle scans that start from either end of the tree when no boundary keys are available
- Supports cross-type comparisons and handles opfamily procedure lookups
- Essential for implementing PostgreSQL's index scan access methods efficiently