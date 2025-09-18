# BitmapHeapNext

## Location
src/backend/executor/nodeBitmapHeapscan.c: 69 - 345

## Overview
Retrieves the next tuple from a BitmapHeapScan node by iterating through bitmap results and fetching tuples from the underlying table relation.

## Definition
```c
static TupleTableSlot *
BitmapHeapNext(BitmapHeapScanState *node)
```

## Detailed Description
BitmapHeapNext is the core tuple-fetching function for bitmap heap scans in PostgreSQL. It implements an efficient scan strategy that uses a bitmap of tuple identifiers (TIDs) obtained from index scans to directly access relevant heap pages. The function handles both regular sequential bitmap iteration and parallel bitmap scans, with sophisticated prefetching mechanisms to optimize I/O performance.

The function operates in two main phases:
1. **Initialization phase**: Sets up bitmap iterators, table scan descriptors, and prefetching infrastructure when called for the first time
2. **Iteration phase**: Continuously fetches pages based on bitmap results, retrieves tuples from those pages, and applies recheck conditions for lossy bitmap entries

For prefetching optimization, the function maintains two separate iterators - one for actual scanning and another that runs ahead for prefetching pages. The prefetch distance starts small and gradually increases to avoid unnecessary I/O in queries that terminate early due to LIMIT clauses.

## Parameters / Member Variables
- `node`: BitmapHeapScanState containing scan state information including:
  - `tbm`: The TIDBitmap containing page/tuple location information
  - `tbmiterator`/`shared_tbmiterator`: Iterator(s) for traversing bitmap results
  - `tbmres`: Current bitmap iteration result
  - `prefetch_*`: Prefetching control variables
  - `initialized`: Flag indicating whether scan setup is complete

## Dependencies
- Functions called/Symbols referenced:
  - `[MultiExecProcNode](../M/MultiExecProcNode.md)`: Execute child index scan to obtain bitmap
  - `[tbm_begin_iterate](../t/tbm_begin_iterate.md)`/`tbm_prepare_shared_iterate`: Initialize bitmap iterators
  - `[tbm_iterate](../t/tbm_iterate.md)`/`tbm_shared_iterate`: Get next bitmap page result
  - `[BitmapAdjustPrefetchIterator](BitmapAdjustPrefetchIterator.md)`: Adjust prefetch iterator position
  - `[BitmapAdjustPrefetchTarget](BitmapAdjustPrefetchTarget.md)`: Adjust prefetch distance target
  - `[BitmapPrefetch](BitmapPrefetch.md)`: Issue prefetch requests for upcoming pages
  - `[table_beginscan_bm](../t/table_beginscan_bm.md)`: Initialize bitmap table scan
  - `table_scan_bitmap_next_block`: Position scanner at bitmap result block
  - `table_scan_bitmap_next_tuple`: Fetch next tuple from current block
  - `ExecQualAndReset`: Apply recheck conditions for lossy bitmap entries
  - `[BitmapDoneInitializingSharedState](BitmapDoneInitializingSharedState.md)`: Signal completion of shared state setup
- Called from (representative examples):
  - `[ExecBitmapHeapScan](../E/ExecBitmapHeapScan.md)`: Main execution function for BitmapHeapScan nodes

## Notes and Other Information
- The function supports both exact and lossy bitmap scans, with recheck logic for lossy entries
- Prefetching is conditionally compiled with USE_PREFETCH and uses adaptive prefetch distance control
- For parallel execution, the function coordinates shared state initialization among worker processes
- The scan can handle cases where no tuple data is needed (projection-only queries) by using table_beginscan_bm appropriately
- Error handling ensures that unexpected results from child index scans are detected and reported