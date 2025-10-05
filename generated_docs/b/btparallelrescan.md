# btparallelrescan

## Location
[src/backend/access/nbtree/nbtree.c:561-603](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtree.c#L561-L603)

## Overview
Resets a parallel btree scan to its initial state, allowing the scan to be restarted from the beginning.

## Definition
```c
void btparallelrescan(IndexScanDesc scan)
```

## Detailed Description
This function resets the state of a parallel btree scan by reinitializing the shared BTParallelScanDesc structure. It sets the scan page back to invalid and marks the page status as not initialized, effectively preparing the parallel scan to restart from the beginning. 

The function acquires a spinlock for consistency even though, in theory, no other workers should be running at the time of rescan. This ensures thread safety and maintains consistent locking patterns throughout the parallel scan infrastructure.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure containing the parallel scan information to be reset

## Dependencies
- Functions called/Symbols referenced:
  - [BTParallelScanDesc](../B/BTParallelScanDesc.md) (type)
  - [ParallelIndexScanDesc](../P/ParallelIndexScanDesc.md) (type)
  - OffsetToPointer (function)
  - SpinLockAcquire (function)
  - InvalidBlockNumber (constant)
  - BTPARALLEL_NOT_INITIALIZED (constant)
  - SpinLockRelease (function)
- Called from (representative examples):
  - [bthandler](bthandler.md)

## Notes and Other Information
- The function assumes that parallel_scan is not NULL (enforced by Assert)
- Spinlock acquisition is done for consistency even when other workers shouldn't be active
- After reset, the parallel scan can be restarted with new scan keys or parameters
- The BTParallelScanDesc is accessed through an offset from the ParallelIndexScanDesc base
- This function is part of the btree index access method's parallel scan infrastructure

## Simplified Source

```c
void btparallelrescan(IndexScanDesc scan) {
    BTParallelScanDesc btscan;
    ParallelIndexScanDesc parallel_scan = scan->parallel_scan;

    // Get parallel scan descriptor from offset
    btscan = (BTParallelScanDesc) OffsetToPointer((void *) parallel_scan,
                                                  parallel_scan->ps_offset);

    // Reset scan state with mutex protection for consistency
    SpinLockAcquire(&btscan->btps_mutex);
    btscan->btps_scanPage = InvalidBlockNumber;
    btscan->btps_pageStatus = BTPARALLEL_NOT_INITIALIZED;
    SpinLockRelease(&btscan->btps_mutex);
}
```