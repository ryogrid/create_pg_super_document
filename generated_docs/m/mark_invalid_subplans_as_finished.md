# mark_invalid_subplans_as_finished

## Location
src/backend/executor/nodeAppend.c: 828 - 861

## Overview
Marks invalid subplans as finished in the ParallelAppendState during runtime partition pruning, ensuring that parallel workers skip subplans that have been determined to be unnecessary for query execution.

## Definition


## Detailed Description
This function is a crucial component of PostgreSQL's runtime partition pruning mechanism for parallel Append nodes. When runtime pruning determines that certain subplans are not needed (e.g., partitions that don't match the query's WHERE conditions), this function marks those invalid subplans as finished in the parallel execution state.

The function operates by iterating through all subplans and checking each one against the valid subplans bitmap. Any subplan not present in the valid subplans set is marked as finished, preventing parallel workers from attempting to execute unnecessary subplans. This optimization reduces wasted work and improves query performance by eliminating execution of subplans that cannot contribute results.

The function includes safety checks to ensure it's only called in appropriate contexts - specifically when parallel execution is active and runtime pruning is enabled. It also includes an early return optimization when all subplans are valid.

## Parameters / Member Variables
- : Pointer to AppendState containing the append node's execution state, valid subplans bitmap, and parallel state information for tracking finished subplans

## Dependencies
- Functions called/Symbols referenced:
  - bms_num_members (bitmap membership counting)
  - bms_is_member (bitmap membership testing)
- Called from (representative examples):
  - choose_next_subplan_for_leader (leader process initialization)
  - choose_next_subplan_for_worker (worker process initialization)

## Notes and Other Information
- Only called in parallel Append mode with runtime pruning enabled
- Provides an early return optimization when all subplans are valid (no pruning occurred)
- Essential for the efficiency of partitioned table queries with runtime pruning
- Works in conjunction with ExecFindMatchingSubPlans to implement complete runtime pruning
- Part of PostgreSQL's partition-wise join and partition pruning optimization features
- Helps prevent unnecessary I/O and CPU usage in parallel query execution