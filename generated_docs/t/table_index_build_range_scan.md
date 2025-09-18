# table_index_build_range_scan

## Location
src/include/access/tableam.h: 1809 - 1839

## Overview
Scans a specified range of blocks in a table during index construction, with options for visibility mode and synchronization control.

## Definition
```c
static inline double
table_index_build_range_scan(Relation table_rel,
                             Relation index_rel,
                             struct IndexInfo *index_info,
                             bool allow_sync,
                             bool anyvisible,
                             bool progress,
                             BlockNumber start_blockno,
                             BlockNumber numblocks,
                             IndexBuildCallback callback,
                             void *callback_state,
                             TableScanDesc scan)
```

## Detailed Description
This function provides a more flexible version of `table_index_build_scan()` that allows scanning only a specified range of blocks instead of the entire table. It's particularly useful for parallel index builds, incremental operations, or when processing specific portions of large tables.

The function supports two visibility modes: standard mode where only tuples visible to the current transaction are processed, and "anyvisible" mode where all tuples visible to any transaction are indexed and counted as live, including those from in-progress transactions.

Range restrictions cannot be used in combination with synchronized scanning (`allow_sync` must be false when scanning a partial range), as syncscan optimization requires access to the full table.

## Parameters / Member Variables
- `table_rel`: Relation - The parent table relation being scanned
- `index_rel`: Relation - The index relation being built  
- `index_info`: struct IndexInfo* - Information about the index being constructed
- `allow_sync`: bool - Whether to allow synchronized scanning (must be false for range scans)
- `anyvisible`: bool - If true, index tuples visible to any transaction; if false, use standard visibility rules
- `progress`: bool - Whether to report progress through the progress reporting system
- `start_blockno`: BlockNumber - First block number to scan
- `numblocks`: BlockNumber - Number of blocks to scan (InvalidBlockNumber means scan to end-of-relation)
- `callback`: IndexBuildCallback - Function called for each tuple to add it to the index
- `callback_state`: void* - State data passed to the callback function
- `scan`: TableScanDesc - Table scan descriptor for the operation

## Dependencies
- Functions called/Symbols referenced:
  - table_rel->rd_tableam->index_build_range_scan (delegates to table AM implementation)
- Types referenced:
  - Relation
  - IndexInfo  
  - BlockNumber
  - IndexBuildCallback
  - TableScanDesc
- Called from (representative examples):
  - summarize_range (src/backend/access/brin/brin.c:1808)

## Notes and Other Information
- Returns the total count of live tuples scanned in the specified range
- Cannot combine range restrictions with synchronized scanning
- The "anyvisible" mode is useful for operations that need to see all data regardless of transaction boundaries
- Passing InvalidBlockNumber as numblocks scans from start_blockno to end-of-relation
- Part of the table access method abstraction layer
- Used internally by `table_index_build_scan()` with full table parameters
- Primarily used by BRIN indexes for summarizing specific block ranges