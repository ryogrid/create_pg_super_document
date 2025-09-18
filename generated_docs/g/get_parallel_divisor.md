# get_parallel_divisor

## Location
src/backend/optimizer/path/costsize.c: 6366 - 6405

## Overview
Estimates the effective parallelism divisor by calculating the fraction of work each worker (including the leader) contributes to a parallel operation.

## Definition
```c
static double get_parallel_divisor(Path *path)
```

## Detailed Description
This function calculates how much parallel execution will speed up a given operation by determining the effective parallelism factor. It accounts for the fact that the leader process becomes less effective as more workers are added, since it spends increasing time coordinating workers rather than executing the parallel plan itself. The function uses a heuristic where the leader spends 30% of its time servicing each worker, with the remainder available for executing the parallel portion of the plan. When there are 4 or more workers, the leader no longer makes a meaningful contribution to parallel execution.

## Parameters / Member Variables
- `path`: Pointer to a Path structure containing information about the planned operation, including the number of parallel workers budgeted

## Dependencies
- Functions called/Symbols referenced:
  - parallel_leader_participation (global variable controlling leader participation)
  - Path->parallel_workers (member of Path structure)
- Called from (representative examples):
  - cost_qual_eval_context
  - cost_seqscan
  - cost_index
  - cost_bitmap_heap_scan
  - cost_append
  - final_cost_nestloop
  - final_cost_mergejoin
  - initial_cost_hashjoin
  - final_cost_hashjoin

## Notes and Other Information
- This is a static function used internally within the cost estimation module
- The 30% overhead factor per worker is based on early experience with parallel query execution
- The function handles the case where leader contribution might become negative (when there are many workers)
- Critical for accurate cost estimation of parallel operations in query planning
- The parallel_leader_participation setting can disable leader participation entirely
- Used across all major parallel operation types including scans, joins, and aggregations