# _bt_parallel_heapscan

## Location
src/backend/access/nbtree/nbtsort.c: 1653 - 1686

## Overview
Coordinates the completion of parallel heap scanning by waiting for all worker processes to finish and collecting aggregated statistics from the parallel build operation.

## Definition


## Detailed Description
This function serves as the synchronization point for the leader process during parallel B-tree index construction. It waits for all participating worker processes (including the leader if it participates as a worker) to complete their portion of the heap scan and tuple processing.

The function uses a condition variable-based waiting mechanism with spinlock-protected access to shared state. It continuously polls the shared state to check if all participants have completed their work, sleeping on a condition variable between checks to avoid busy waiting.

Once all workers are done, the function aggregates important build statistics from the shared state:
- Total number of heap tuples processed
- Number of index tuples created
- Whether any dead tuples were encountered
- Whether any broken HOT (Heap-Only Tuple) chains were detected

Key responsibilities include:
- Monitoring completion status of all parallel workers
- Safely accessing shared statistics under spinlock protection
- Providing aggregated build statistics to the caller
- Ensuring proper condition variable cleanup

## Parameters / Member Variables
- : Main B-tree build state containing the BTLeader with shared state access
- : Output parameter set to true if any worker detected broken HOT chains

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire/SpinLockRelease: Protect access to shared state variables
  - ConditionVariableSleep: Wait for worker completion notification
  - ConditionVariableCancelSleep: Clean up condition variable wait state
  - BTShared: Shared state structure containing coordination and statistics
- Called from (representative examples):
  - _bt_spools_heapscan: Main heap scanning coordinator after parallel workers are launched

## Notes and Other Information
- Uses WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN for wait event monitoring
- Critical synchronization point ensuring all parallel work completes before index finalization
- Returns the total count of heap tuples processed across all workers
- Broken HOT chain detection is important for index build correctness
- Statistics aggregation enables proper reporting of parallel build performance
- Must be called after _bt_begin_parallel() has successfully launched workers