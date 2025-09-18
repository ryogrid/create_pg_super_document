# _bt_parallel_release

## Location
[src/backend/access/nbtree/nbtree.c:713-735](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtree.c#L713-L735)

## Overview
Releases control of a parallel btree scan after advancing it to a new page, allowing other worker processes to continue.

## Definition
```c
void _bt_parallel_release(IndexScanDesc scan, BlockNumber scan_page)
```

## Detailed Description
This function completes the process of advancing a parallel btree scan to a new page. After a worker process has successfully seized control using  and advanced to a new page, it calls this function to release control and update the shared scan state with the new page number. 

The function updates the shared BTParallelScanDesc structure with the new scan page and changes the status to BTPARALLEL_IDLE, indicating that other workers can now seize control. It then signals any waiting workers through the condition variable to wake them up and allow them to proceed.

For scans using array keys, the caller must save the scan_page argument for potential use with  if another primitive index scan becomes necessary.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure containing the parallel scan information
- `scan_page`: The block number of the page that the scan has advanced to

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelIndexScanDesc](../P/ParallelIndexScanDesc.md) (type)
  - [BTParallelScanDesc](../B/BTParallelScanDesc.md) (type)
  - OffsetToPointer (function)
  - SpinLockAcquire/SpinLockRelease (functions)
  - BTPARALLEL_IDLE (constant)
  - ConditionVariableSignal (function)
- Called from (representative examples):
  - [_bt_readpage](_bt_readpage.md)
  - [_bt_readnextpage](_bt_readnextpage.md)

## Notes and Other Information
- Must be called after successfully seizing the scan with 
- Updates the shared scan page to the new position and sets status to idle
- Signals waiting workers through the condition variable to allow them to proceed
- For array key scans, the scan_page may be skipped if a new primitive scan is required
- Critical for maintaining proper synchronization in parallel btree scans
- The scan_page parameter becomes the new shared scan position for other workers