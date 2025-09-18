# BitmapAdjustPrefetchIterator

## Location
src/backend/executor/nodeBitmapHeapscan.c: 358 - 422

## Overview
Adjusts the prefetch iterator position to maintain proper synchronization with the main bitmap iterator and manages prefetch distance tracking.

## Definition
```c
static inline void
BitmapAdjustPrefetchIterator(BitmapHeapScanState *node,
                            BlockNumber blockno)
```

## Detailed Description
BitmapAdjustPrefetchIterator manages the synchronization between the main bitmap iterator and the prefetch iterator in bitmap heap scans. The function ensures that the prefetch iterator stays appropriately ahead of the main iterator to enable effective I/O prefetching, while preventing the iterators from becoming desynchronized.

The function operates differently depending on whether the scan is running in parallel mode or not:

**Non-parallel mode**: When prefetch pages are available, it decrements the prefetch distance counter. When the prefetch iterator falls behind the main iterator, it advances the prefetch iterator and validates that both iterators are processing the same block to ensure synchronization.

**Parallel mode**: Uses spinlock-protected shared state to manage prefetch distance across multiple processes. In shared mode, the function doesn't validate block number synchronization since different processes may be working on different blocks simultaneously, making strict synchronization impossible.

The function is conditionally compiled with USE_PREFETCH and is designed to be called each time the main iterator advances to a new block.

## Parameters / Member Variables
- `node`: BitmapHeapScanState containing scan state and prefetch configuration:
  - `prefetch_iterator`/`shared_prefetch_iterator`: Iterator running ahead for prefetching
  - `prefetch_pages`: Current distance between main and prefetch iterators
  - `prefetch_maximum`: Maximum allowed prefetch distance
  - `pstate`: Parallel state information (NULL for non-parallel scans)
- `blockno`: Block number that the main iterator is currently processing

## Dependencies
- Functions called/Symbols referenced:
  - `tbm_iterate`: Advance the non-parallel prefetch iterator
  - `tbm_shared_iterate`: Advance the shared prefetch iterator in parallel mode
  - `SpinLockAcquire`/`SpinLockRelease`: Protect shared state modifications in parallel mode
  - `elog`: Report synchronization errors in non-parallel mode
- Called from (representative examples):
  - `BitmapHeapNext`: Called when advancing to each new bitmap result block

## Notes and Other Information
- Only compiled when USE_PREFETCH is defined, making prefetching optional
- Synchronization validation is only performed in non-parallel mode due to the inherent challenges of coordinating multiple processes
- The function uses spinlocks minimally in parallel mode to reduce contention
- Error detection helps identify bugs in iterator management logic during development
- The inline designation indicates this is a performance-critical function called frequently during scans