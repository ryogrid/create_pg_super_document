# btinitparallelscan

## Location
src/backend/access/nbtree/nbtree.c: 547 - 560

## Overview
Initializes a BTParallelScanDesc structure for coordinating parallel btree index scans across multiple worker processes.

## Definition
```c
void btinitparallelscan(void *target)
```

## Detailed Description
This function initializes the shared data structure used for coordinating parallel btree scans. It sets up synchronization primitives (spinlock and condition variable) and initializes the scan state to indicate that the parallel scan has not yet been initialized. The BTParallelScanDesc structure will be used by multiple worker processes to coordinate their scanning efforts on the btree index.

The initialization includes setting up the mutex for protecting shared state, marking the current scan page as invalid, setting the initial page status, and initializing the condition variable used for worker coordination.

## Parameters / Member Variables
- `target`: Pointer to the BTParallelScanDesc structure to be initialized (cast from void*)

## Dependencies
- Functions called/Symbols referenced:
  - BTParallelScanDesc (type cast)
  - SpinLockInit (function)
  - InvalidBlockNumber (constant)
  - BTPARALLEL_NOT_INITIALIZED (constant)
  - ConditionVariableInit (function)
- Called from (representative examples):
  - bthandler

## Notes and Other Information
- This function is called once per parallel scan to initialize the shared coordination structure
- The initialized structure will be placed in shared memory accessible to all parallel workers
- The spinlock protects access to the scan page and status information
- The condition variable allows workers to wait for and signal scan state changes
- Initial page status of BTPARALLEL_NOT_INITIALIZED indicates the scan hasn't started yet