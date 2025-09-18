# cost_append

## Location
src/backend/optimizer/path/costsize.c: 2231 - 2403

## Overview
Determines and returns the cost of an Append node, which combines results from multiple child paths either sequentially or in parallel.

## Definition


## Detailed Description
This function calculates the execution cost for an Append operation, handling three distinct scenarios:

1. **Unordered, non-parallel-aware Append**: Simple summation of child costs with startup cost from the first subpath.

2. **Ordered, non-parallel-aware Append**: Sums startup costs of all subpaths to handle cases where multiple children must run to satisfy a LIMIT clause. Injects Sort nodes for subpaths that don't match the required ordering.

3. **Parallel-aware Append**: Complex cost calculation involving:
   - Startup cost as minimum among initially assigned workers
   - Parallel divisor scaling for partial paths
   - Load balancing cost calculation via append_nonpartial_cost()
   - Row count adjustments based on parallel execution ratios

The function also adds a small per-tuple overhead cost using APPEND_CPU_COST_MULTIPLIER to account for the Append node's processing.

## Parameters / Member Variables
- : AppendPath object containing subpaths and configuration, which gets updated with calculated costs and row estimates

## Dependencies
- Functions called/Symbols referenced:
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md)
  - [cost_sort](cost_sort.md)
  - [get_parallel_divisor](../g/get_parallel_divisor.md)
  - [clamp_row_est](clamp_row_est.md)
  - [append_nonpartial_cost](../a/append_nonpartial_cost.md)
  - APPEND_CPU_COST_MULTIPLIER
  - AppendPath (struct)
- Called from (representative examples):
  - [create_append_path](create_append_path.md)

## Notes and Other Information
- Returns early if subpaths list is empty (NIL)
- For ordered appends, conservatively sums all startup costs to avoid underestimating LIMIT query costs
- Parallel-aware appends never produce ordered output (assertion enforces this)
- Uses clamp_row_est to ensure row estimates remain within reasonable bounds
- Handles both partial and non-partial subpaths differently in parallel mode
- Injects Sort nodes automatically when subpaths don't match required ordering
- Critical for choosing between different append strategies in the query planner