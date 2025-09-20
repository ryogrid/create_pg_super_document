# append_startup_cost_compare

## Location
[src/backend/optimizer/util/pathnode.c:1397-1414](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L1397-L1414)

## Overview
A list_sort comparator function that sorts append child paths by startup_cost in descending order for parallel append operations on partial paths.

## Definition

```c
static int
append_startup_cost_compare(const ListCell *a, const ListCell *b)
```
## Detailed Description
This static function serves as a comparison function for list_sort to order append child paths by their startup costs in descending order. This sorting is specifically used for partial subpaths in parallel append operations. The rationale is that some partial paths may require startup work to be done by a single worker, so it's better for workers to choose the expensive startup plans first, while the leader should choose the cheapest startup plan.

The function implements a multi-level comparison strategy: first by startup cost (descending), then falls back to total cost comparison if startup costs are equal, and finally uses relids comparison to ensure deterministic results.

## Parameters / Member Variables
- : First ListCell containing a Path pointer for comparison
- : Second ListCell containing a Path pointer for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [compare_path_costs](../c/compare_path_costs.md) (with STARTUP_COST flag)
  - [bms_compare](../b/bms_compare.md) (for breaking ties using relation IDs)
  - lfirst (macro for extracting list cell contents)
- Called from (representative examples):
  - [create_append_path](../c/create_append_path.md) (via list_sort for sorting partial_subpaths in parallel-aware append)

## Notes and Other Information
- This is a static function only used within pathnode.c
- Uses descending order (-cmp) to prioritize expensive startup paths first for worker processes
- Complements append_total_cost_compare: one sorts regular subpaths by total cost, this sorts partial subpaths by startup cost
- Falls back to total cost comparison and then relids comparison for deterministic sorting
- Part of the parallel append optimization where workers should handle expensive startup work first
- Ensures consistent, reproducible sorting results when costs are identical