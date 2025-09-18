# append_total_cost_compare

## Location
[src/backend/optimizer/util/pathnode.c:1375-1396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L1375-L1396)

## Overview
A list_sort comparator function that sorts append child paths by total_cost in descending order for parallel append operations.

## Definition


## Detailed Description
This static function serves as a comparison function for list_sort to order append child paths by their total costs in descending order. This sorting is specifically used in parallel append operations to minimize the total time needed to finish all non-partial paths. The function implements a multi-level comparison strategy: first by total cost (descending), then by startup cost if total costs are equal, and finally by relids comparison to ensure deterministic results.

The descending order ensures that the most expensive paths are processed first, which helps balance the workload across parallel workers and reduces overall execution time.

## Parameters / Member Variables
- : First ListCell containing a Path pointer for comparison
- : Second ListCell containing a Path pointer for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [compare_path_costs](../c/compare_path_costs.md) (with TOTAL_COST flag)
  - [bms_compare](../b/bms_compare.md) (for breaking ties using relation IDs)
  - lfirst (macro for extracting list cell contents)
- Called from (representative examples):
  - [create_append_path](../c/create_append_path.md) (via list_sort for parallel-aware append paths)

## Notes and Other Information
- This is a static function only used within pathnode.c
- Uses descending order (-cmp) to prioritize expensive paths first in parallel execution
- Falls back to relids comparison to ensure deterministic sorting when costs are identical
- Part of the parallel append optimization strategy where expensive non-partial paths should be started first
- The comparison logic ensures that list_sort produces consistent, reproducible results