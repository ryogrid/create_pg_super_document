# BitmapAdjustPrefetchTarget

## Location
src/backend/executor/nodeBitmapHeapscan.c: 423 - 461

## Overview
Dynamically adjusts the prefetch target distance using an adaptive algorithm that starts small and gradually increases to optimize I/O performance.

## Definition
```c
static inline void
BitmapAdjustPrefetchTarget(BitmapHeapScanState *node)
```

## Detailed Description
BitmapAdjustPrefetchTarget implements an adaptive prefetching algorithm that dynamically adjusts the desired prefetch distance based on scan progress. The function uses a conservative growth strategy to avoid unnecessary I/O overhead in scans that may terminate early due to LIMIT clauses or other constraints.

The algorithm follows a specific growth pattern:
1. **Initial state**: prefetch_target starts at -1 (no prefetching)
2. **First adjustment**: Increases to 0 after fetching the first page/tuple
3. **Second adjustment**: Increases to 1 after processing continues
4. **Subsequent adjustments**: Doubles the target until reaching half the maximum, then jumps directly to the maximum

This graduated approach ensures that short-running queries don't waste I/O bandwidth on unnecessary prefetching, while longer scans benefit from aggressive prefetching once their continuation pattern is established.

For parallel scans, the function uses optimistic concurrency control with an unlocked check followed by spinlock-protected updates to minimize contention while ensuring consistency across worker processes.

## Parameters / Member Variables
- `node`: BitmapHeapScanState containing prefetch control variables:
  - `prefetch_target`: Current desired prefetch distance (modified by this function)
  - `prefetch_maximum`: Maximum allowed prefetch distance (configuration limit)
  - `pstate`: Parallel state for coordinating shared prefetch target across workers

## Dependencies
- Functions called/Symbols referenced:
  - `SpinLockAcquire`/`SpinLockRelease`: Protect shared state updates in parallel mode
- Called from (representative examples):
  - `BitmapHeapNext`: Called after successfully processing each bitmap result block

## Notes and Other Information
- Only compiled when USE_PREFETCH is defined, making prefetching optional
- Uses optimistic concurrency in parallel mode to reduce spinlock contention
- The growth algorithm is designed to balance early termination efficiency with long-scan performance
- The inline designation indicates this is called frequently during bitmap scans
- Prefetch target adjustments are made per-block rather than per-tuple to avoid excessive overhead
- The algorithm prevents target from exceeding the configured maximum to avoid unbounded resource consumption