# table_index_build_scan

## Location
[src/include/access/tableam.h:1776-1808](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1776-L1808)

## Overview
Scans a table to find tuples that should be indexed, calling a callback function for each suitable tuple during index construction.

## Definition
```c
static inline double
table_index_build_scan(Relation table_rel,
                       Relation index_rel,
                       struct IndexInfo *index_info,
                       bool allow_sync,
                       bool progress,
                       IndexBuildCallback callback,
                       void *callback_state,
                       TableScanDesc scan)
```

## Detailed Description
This function is the primary interface for scanning a table during index build operations. It serves as a wrapper around the table access method's `index_build_range_scan` implementation, scanning the entire table from beginning to end. For each tuple that should be entered into the index, it calls the provided callback function which handles the access method-specific logic for adding the tuple to the new index.

The function tracks and detects potentially broken HOT (Heap-Only Tuple) chains by setting `indexInfo->ii_BrokenHotChain` to true when RECENTLY_DEAD or DELETE_IN_PROGRESS entries are found in HOT chains. This detection is primarily relevant for heap access methods but may need generalization for other access methods.

Progress reporting is optionally supported, updating PROGRESS_SCAN_BLOCKS_TOTAL at scan start and PROGRESS_SCAN_BLOCKS_DONE during execution.

## Parameters / Member Variables
- `table_rel`: Relation - The parent table relation being scanned
- `index_rel`: Relation - The index relation being built
- `index_info`: struct IndexInfo* - Information about the index being constructed
- `allow_sync`: bool - Whether to allow synchronized scanning optimizations
- `progress`: bool - Whether to report progress through the progress reporting system
- `callback`: IndexBuildCallback - Function called for each tuple to add it to the index
- `callback_state`: void* - State data passed to the callback function
- `scan`: TableScanDesc - Table scan descriptor for the operation

## Dependencies
- Functions called/Symbols referenced:
  - table_rel->rd_tableam->index_build_range_scan (delegates to table AM implementation)
- Types referenced:
  - [Relation](../R/Relation.md)
  - IndexInfo
  - IndexBuildCallback
  - [TableScanDesc](../T/TableScanDesc.md)
- Called from (representative examples):
  - [brinbuild](../b/brinbuild.md) (src/backend/access/brin/brin.c:1223)
  - [_brin_parallel_scan_and_build](../b/_brin_parallel_scan_and_build.md) (src/backend/access/brin/brin.c:2823)
  - [ginbuild](../g/ginbuild.md) (src/backend/access/gin/gininsert.c:382)
  - [gistbuild](../g/gistbuild.md) (src/backend/access/gist/gistbuild.c:274, 313)
  - [hashbuild](../h/hashbuild.md) (src/backend/access/hash/hash.c:173)
  - [_bt_spools_heapscan](../b/_bt_spools_heapscan.md) (src/backend/access/nbtree/nbtsort.c:475)
  - [_bt_parallel_scan_and_sort](../b/_bt_parallel_scan_and_sort.md) (src/backend/access/nbtree/nbtsort.c:1925)
  - [spgbuild](../s/spgbuild.md) (src/backend/access/spgist/spginsert.c:124)

## Notes and Other Information
- Returns the total count of live tuples for updating pg_class statistics
- The index access method must track the number of index tuples separately, as some tuples may be rejected
- Internally delegates to `index_build_range_scan` with range parameters set to scan the entire table (numblocks=false, start_blockno=0, end_blockno=InvalidBlockNumber)
- Side effect: Sets `indexInfo->ii_BrokenHotChain` when potentially broken HOT chains are detected
- Part of the table access method abstraction layer
- Used by all major index types during their build processes