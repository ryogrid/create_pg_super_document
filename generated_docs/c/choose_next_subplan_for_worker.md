# choose_next_subplan_for_worker

## Location
src/backend/executor/nodeAppend.c: 702 - 827

## Overview
Chooses the next subplan for a parallel-aware Append node to execute, coordinating work distribution among parallel workers by managing the selection and assignment of subplans in a thread-safe manner.

## Definition


## Detailed Description
This function implements the core work distribution logic for parallel-aware Append nodes in PostgreSQL's executor. It operates under exclusive locking to ensure thread-safe coordination among multiple parallel workers. The function follows a specific strategy for subplan assignment:

1. **Non-partial plans first**: Assigns non-partial plans in order of descending cost, with each plan executed by a single worker
2. **Partial plan distribution**: After non-partial plans are exhausted, distributes partial plans evenly across available workers
3. **Cyclic assignment**: When reaching the end of valid subplans, loops back to the first partial plan to ensure even work distribution

The function handles runtime partition pruning by identifying valid subplans on the first call and marking invalid subplans as finished. It maintains state through the ParallelAppendState structure, tracking which subplans are completed and determining the next available subplan for execution.

## Parameters / Member Variables
- : Pointer to AppendState containing the append node's execution state, parallel state information, and subplan tracking data

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsForward (direction validation)
  - LWLockAcquire/LWLockRelease (thread synchronization)
  - [ExecFindMatchingSubPlans](../E/ExecFindMatchingSubPlans.md) (runtime pruning support)
  - [mark_invalid_subplans_as_finished](../m/mark_invalid_subplans_as_finished.md) (pruning cleanup)
  - [bms_next_member](../b/bms_next_member.md) (bitmap set iteration)
- Called from (representative examples):
  - [ExecAppendInitializeWorker](../E/ExecAppendInitializeWorker.md) (worker initialization)

## Notes and Other Information
- Only supports forward scans (backward scans are not supported in parallel-aware plans)
- Uses exclusive locking on pa_lock to coordinate between parallel workers
- Immediately marks non-partial plans as finished since they cannot be shared between workers
- Returns false when no more subplans are available for execution
- Critical for load balancing in parallel query execution
- Part of PostgreSQL's parallel query execution infrastructure introduced for improved performance on multi-core systems