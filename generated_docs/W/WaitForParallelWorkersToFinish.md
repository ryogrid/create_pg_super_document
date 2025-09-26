# WaitForParallelWorkersToFinish

## Location
src/backend/access/transam/parallel.c: 792 - 905

## Overview
Waits for all parallel workers to complete their computations and handles any errors they may have encountered during execution or shutdown.

## Definition
```c
void WaitForParallelWorkersToFinish(ParallelContext *pcxt)
```

## Detailed Description
This function ensures all parallel workers have finished their assigned work and collects any error messages they may have generated. It serves several critical purposes:

1. **Error Collection**: Processes pending parallel messages and propagates any worker errors to the leader
2. **Completion Verification**: Ensures all workers have cleanly finished their computations
3. **Transaction Coordination**: Updates XactLastRecEnd based on worker feedback for transaction consistency
4. **Failure Detection**: Identifies workers that failed to initialize or terminated abnormally

The function continuously polls worker status until all are confirmed finished, handling cases where workers may not have started yet or failed during startup. It uses latches for efficient waiting and processes interrupts to handle error propagation.

## Parameters / Member Variables
- `pcxt`: Pointer to the ParallelContext containing information about all launched workers

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - GetBackgroundWorkerPid
  - shm_mq_get_queue
  - shm_mq_get_sender
  - WaitLatch
  - ResetLatch
  - shm_toc_lookup
  - ereport
- Called from (representative examples):
  - _brin_end_parallel
  - _bt_end_parallel
  - ExecParallelFinish
  - parallel_vacuum_process_all_indexes

## Notes and Other Information
- Essential function that must be called after parallel operations to ensure correctness
- Handles both successful completion and various failure scenarios
- Updates transaction state (XactLastRecEnd) from worker feedback when applicable
- Uses WAIT_EVENT_PARALLEL_FINISH for wait event monitoring
- Critical for maintaining transaction consistency and proper error handling in parallel operations