# BitmapDoneInitializingSharedState

## Location
src/backend/executor/nodeBitmapHeapscan.c: 346 - 357

## Overview
Signals completion of shared state initialization in parallel bitmap heap scans by updating the state and waking up waiting worker processes.

## Definition
```c
static inline void
BitmapDoneInitializingSharedState(ParallelBitmapHeapState *pstate)
```

## Detailed Description
BitmapDoneInitializingSharedState is a synchronization function used in parallel bitmap heap scans to coordinate between the leader process and worker processes. When a parallel bitmap heap scan begins, the leader process is responsible for executing the underlying index scan to build the TIDBitmap and setting up the shared iteration state. Meanwhile, worker processes wait until this initialization is complete.

This function marks the completion of the initialization phase by setting the shared state to BM_FINISHED and broadcasting a condition variable to wake up all waiting worker processes. The function uses a spinlock to ensure atomic updates to the shared state, preventing race conditions during the transition.

The synchronization mechanism ensures that worker processes don't attempt to access uninitialized shared data structures, maintaining consistency across all participating processes in the parallel scan.

## Parameters / Member Variables
- `pstate`: Pointer to ParallelBitmapHeapState containing:
  - `mutex`: Spinlock protecting shared state modifications
  - `state`: Current initialization state (set to BM_FINISHED)  
  - `cv`: Condition variable used to signal waiting processes

## Dependencies
- Functions called/Symbols referenced:
  - `SpinLockAcquire`/`SpinLockRelease`: Acquire and release spinlock for atomic state updates
  - `ConditionVariableBroadcast`: Wake up all processes waiting on the condition variable
  - `BM_FINISHED`: Enumeration value indicating completed initialization
- Called from (representative examples):
  - `BitmapHeapNext`: Called after leader completes shared state setup

## Notes and Other Information
- This function is only called by the leader process in a parallel bitmap heap scan
- The inline designation suggests this is a performance-critical synchronization point
- The spinlock duration is kept minimal to avoid blocking worker processes unnecessarily
- After this function completes, worker processes can safely proceed with their bitmap iteration