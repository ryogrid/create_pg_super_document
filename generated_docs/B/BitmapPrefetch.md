# BitmapPrefetch

## Location
src/backend/executor/nodeBitmapHeapscan.c: 462 - 561

## Overview
Issues prefetch requests for heap pages identified by the bitmap iterator to improve I/O performance by reading ahead of the main scan.

## Definition
```c
static inline void
BitmapPrefetch(BitmapHeapScanState *node, TableScanDesc scan)
```

## Detailed Description
BitmapPrefetch implements the core prefetching logic for bitmap heap scans by issuing asynchronous read requests for pages that will be needed in the near future. The function advances the prefetch iterator ahead of the main iterator and issues PrefetchBuffer calls to warm the buffer cache before those pages are actually accessed.

The function operates differently for non-parallel and parallel scans:

**Non-parallel mode**: Uses a dedicated prefetch iterator to advance through bitmap results. It continues prefetching until the desired prefetch distance (prefetch_target) is achieved or no more pages are available.

**Parallel mode**: Coordinates prefetching across multiple worker processes using spinlock-protected shared state. Each process claims prefetch work by atomically incrementing the shared prefetch counter, preventing duplicate prefetch requests.

The function includes intelligent optimizations to skip prefetching pages that are unlikely to be needed:
- Pages in scans that don't need tuple data (SO_NEED_TUPLES flag)
- Pages that don't require rechecking (exact bitmap matches)
- Pages that are already all-visible according to the visibility map

When no more pages are available for prefetching, the function cleanly terminates the prefetch iterator and sets the iterator pointer to NULL.

## Parameters / Member Variables
- `node`: BitmapHeapScanState containing prefetch control state:
  - `prefetch_iterator`/`shared_prefetch_iterator`: Iterator for prefetch logic
  - `prefetch_pages`: Current prefetch distance from main iterator
  - `prefetch_target`: Desired prefetch distance
  - `pvmbuffer`: Visibility map buffer for optimization decisions
- `scan`: TableScanDesc providing table access context and scan flags

## Dependencies
- Functions called/Symbols referenced:
  - [tbm_iterate](../t/tbm_iterate.md)/`tbm_shared_iterate`: Advance prefetch iterator to get next bitmap result
  - [tbm_end_iterate](../t/tbm_end_iterate.md)/`tbm_end_shared_iterate`: Clean up exhausted iterators
  - [PrefetchBuffer](../P/PrefetchBuffer.md): Issue asynchronous buffer prefetch request to storage layer
  - `VM_ALL_VISIBLE`: Check visibility map to determine if page needs reading
  - `SpinLockAcquire`/`SpinLockRelease`: Coordinate shared prefetch state in parallel mode
- Called from (representative examples):
  - [BitmapHeapNext](BitmapHeapNext.md): Called after determining there's more work on current page

## Notes and Other Information
- Only compiled when USE_PREFETCH is defined, making prefetching completely optional
- Uses MAIN_FORKNUM to specify the main data fork for prefetch requests
- Prefetch requests are issued after determining continued work exists to avoid interference with main I/O
- In parallel mode, uses optimistic concurrency to minimize spinlock contention while ensuring work coordination
- Visibility map optimization prevents unnecessary I/O for pages that are known to be fully visible
- The function gracefully handles iterator exhaustion by cleaning up state and preventing further prefetch attempts