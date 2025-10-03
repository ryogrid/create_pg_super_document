# TidStoreMemoryUsage

## Location
[src/backend/access/common/tidstore.c:551-562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tidstore.c#L551-L562)

## Overview
Returns the total memory usage of a TidStore, delegating to the appropriate implementation based on whether the store is shared or local.

## Definition

```c
size_t
TidStoreMemoryUsage(TidStore *ts)
```
## Detailed Description
This function provides a unified interface to query the memory consumption of a TidStore regardless of its implementation type. It determines whether the TidStore is shared (multi-process) or local (single-process) and calls the appropriate memory usage calculation function. This is essential for memory monitoring, debugging, and resource management in PostgreSQL operations.

The function returns the total memory footprint including the tree structure, bitmap data, and any associated metadata.

## Parameters / Member Variables
- `*ts`: The TidStore for which to calculate memory usage
## Dependencies
- Functions called/Symbols referenced:
  - TidStoreIsShared
  - shared_ts_memory_usage
  - local_ts_memory_usage
- Called from (representative examples):
  - [lazy_scan_heap](../l/lazy_scan_heap.md)
  - [lazy_vacuum](../l/lazy_vacuum.md)
  - [dead_items_add](../d/dead_items_add.md)
  - [test_create](../t/test_create.md)
  - [test_is_full](../t/test_is_full.md)

## Notes and Other Information
- The returned size includes all memory associated with the TidStore structure
- Used extensively in vacuum operations for memory management and progress reporting
- Provides consistent memory reporting across both shared and local TidStore implementations
- Essential for determining when memory limits are approached during vacuum operations
- The function handles the implementation details transparently to the caller