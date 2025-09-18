# _bt_parallel_done

## Location
src/backend/access/nbtree/nbtree.c: 736 - 783

## Overview
Marks a parallel B-tree index scan as complete and notifies other worker processes that no more pages are left to scan.

## Definition


## Detailed Description
This function is called when a parallel B-tree scan has completed and there are no more pages to scan. It updates the parallel scan status to indicate completion and broadcasts this status change to all other worker processes that may be waiting. The function ensures that other workers don't wait indefinitely for the scan to advance to the next page.

The function performs several important operations:
1. Checks if this is actually a parallel scan (returns early if not)
2. Ensures there's no pending primitive index scan before marking as done
3. Atomically updates the parallel scan status using spinlocks
4. Broadcasts the completion status to wake up waiting workers

## Parameters / Member Variables
- : IndexScanDesc structure containing the scan state, including parallel scan information

## Dependencies
- Functions called/Symbols referenced:
  - OffsetToPointer
  - SpinLockAcquire
  - SpinLockRelease
  - ConditionVariableBroadcast
- Types used:
  - IndexScanDesc
  - BTScanOpaque
  - ParallelIndexScanDesc
  - BTParallelScanDesc
- Constants used:
  - BTPARALLEL_NEED_PRIMSCAN
  - BTPARALLEL_DONE
- Called from:
  - _bt_first
  - _bt_readnextpage
  - _bt_start_prim_scan

## Notes and Other Information
- The function uses spinlocks and condition variables for thread-safe coordination between parallel workers
- It includes an assertion to ensure the scan is not in BTPARALLEL_NEED_PRIMSCAN state when marking as done
- The condition variable broadcast ensures all waiting workers are notified simultaneously
- For non-parallel scans, the function returns immediately without doing anything