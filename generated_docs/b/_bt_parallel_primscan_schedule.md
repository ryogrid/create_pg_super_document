# _bt_parallel_primscan_schedule

## Location
src/backend/access/nbtree/nbtree.c: 784 - 820

## Overview
Schedules another primitive index scan in a parallel B-tree scan environment, coordinating array key state between worker processes.

## Definition
```c
void _bt_parallel_primscan_schedule(IndexScanDesc scan, BlockNumber prev_scan_page)
```

## Detailed Description
This function is responsible for scheduling the next primitive index scan in a parallel B-tree scan when array keys are involved. It ensures proper coordination between parallel workers by checking that the shared parallel state hasn't been modified since the caller's last interaction. The function serializes the current array key state to the shared parallel scan descriptor so other workers can continue with the correct array key positioning.

The function operates by:
1. Verifying that the scan page hasn't changed since the caller's last operation
2. Checking that the parallel scan is in an idle state
3. If conditions are met, marking the scan as needing a primitive scan
4. Serializing the current array key elements to shared memory

This is particularly important for queries with array operators (e.g., `column = ANY(array)`) where the scan must iterate through different array elements.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure containing the scan state and parallel scan information
- `prev_scan_page`: The block number that was most recently passed to _bt_parallel_release by the calling backend

## Dependencies
- Functions called/Symbols referenced:
  - OffsetToPointer
  - SpinLockAcquire
  - SpinLockRelease
- Types used:
  - IndexScanDesc
  - BTScanOpaque
  - ParallelIndexScanDesc
  - BTParallelScanDesc
  - BTArrayKeyInfo
  - BlockNumber
- Constants used:
  - BTPARALLEL_IDLE
  - BTPARALLEL_NEED_PRIMSCAN
  - InvalidBlockNumber
- Called from:
  - _bt_advance_array_keys

## Notes and Other Information
- The function includes an assertion requiring that array keys are present (so->numArrayKeys must be non-zero)
- Uses spinlocks for atomic updates to the shared parallel scan state
- The serialization of array key elements ensures that parallel workers can continue with the correct array key positioning
- Only schedules a new primitive scan if the page hasn't changed and the scan is idle, preventing race conditions
- This function is crucial for maintaining consistency in parallel scans with complex predicates involving array operations