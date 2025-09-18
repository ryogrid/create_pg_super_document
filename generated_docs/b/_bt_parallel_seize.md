# _bt_parallel_seize

## Location
src/backend/access/nbtree/nbtree.c: 604 - 712

## Overview
Attempts to seize control of a parallel btree scan to advance it to the next page, coordinating with other worker processes.

## Definition
```c
bool _bt_parallel_seize(IndexScanDesc scan, BlockNumber *pageno, bool first)
```

## Detailed Description
This function implements the core coordination logic for parallel btree scans. It attempts to seize exclusive control of the shared scan state to advance the scan to the next page. Multiple worker processes may call this function concurrently, but only one can successfully seize control at a time.

The function handles three main scenarios:
1. **Scan completion**: Returns false when the parallel scan is done
2. **Primitive scan needed**: Handles array key scans where a new primitive scan must be started
3. **Normal advancement**: Seizes control to advance to the next page

When a primitive scan is needed (for array key operations), only workers calling from  (indicated by ) can start the new primitive scan. Other workers must wait or return false.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure containing the scan state and parallel scan information
- `pageno`: Output parameter that receives the next page number to scan (or InvalidBlockNumber/P_NONE)
- `first`: Boolean indicating if this call is from  (enables starting primitive scans)

## Dependencies
- Functions called/Symbols referenced:
  - BTScanOpaque (type)
  - ParallelIndexScanDesc (type)
  - BTParallelScanDesc (type)
  - OffsetToPointer (function)
  - SpinLockAcquire/SpinLockRelease (functions)
  - ConditionVariableSleep/ConditionVariableCancelSleep (functions)
  - P_NONE, InvalidBlockNumber (constants)
  - BTPARALLEL_DONE, BTPARALLEL_NEED_PRIMSCAN, BTPARALLEL_ADVANCING (status constants)
- Called from (representative examples):
  - _bt_first
  - _bt_steppage
  - _bt_readnextpage

## Notes and Other Information
- Returns true if scan control was successfully seized, false otherwise
- Uses spinlocks for short-term mutual exclusion and condition variables for longer waits
- Handles array key scans by updating scan keys with appropriate array element values
- Updates local backend state (needPrimScan, scanBehind) based on scan progress
- The function may block waiting for other workers to release the scan using condition variables
- Critical for coordinating page-level parallelism in btree index scans