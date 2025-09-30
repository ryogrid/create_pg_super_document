# cost_append

## Location
[src/backend/optimizer/path/costsize.c:2231-2403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L2231-L2403)

## Overview
Determines and returns the cost of an Append node, which combines results from multiple child paths either sequentially or in parallel.

## Definition

```c
void
cost_append(AppendPath *apath)
```
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
  - [AppendPath](../A/AppendPath.md) (struct)
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

## Simplified Source

```c
void cost_append(AppendPath *apath) {
    // Initialize costs and row count
    apath->path.startup_cost = 0;
    apath->path.total_cost = 0;
    apath->path.rows = 0;

    if (apath->subpaths == NIL)
        return;

    if (!apath->path.parallel_aware) {
        if (apath->path.pathkeys == NIL) {
            // Unordered: startup cost from first subpath, sum total costs
            Path *firstsubpath = (Path *) linitial(apath->subpaths);
            apath->path.startup_cost = firstsubpath->startup_cost;

            foreach(l, apath->subpaths) {
                Path *subpath = (Path *) lfirst(l);
                apath->path.rows += subpath->rows;
                apath->path.total_cost += subpath->total_cost;
            }
        } else {
            // Ordered: sum all startup costs, add sort costs if needed
            foreach(l, apath->subpaths) {
                Path *subpath = (Path *) lfirst(l);

                if (!pathkeys_contained_in(pathkeys, subpath->pathkeys)) {
                    // Insert Sort node cost
                    cost_sort(&sort_path, NULL, pathkeys, subpath->total_cost,
                             subpath->rows, subpath->pathtarget->width, 0.0,
                             work_mem, apath->limit_tuples);
                    subpath = &sort_path;
                }

                apath->path.rows += subpath->rows;
                apath->path.startup_cost += subpath->startup_cost;
                apath->path.total_cost += subpath->total_cost;
            }
        }
    } else {
        // Parallel-aware: complex worker assignment and cost calculation
        int i = 0;
        double parallel_divisor = get_parallel_divisor(&apath->path);

        // Calculate startup cost (minimum among first few workers)
        foreach(l, apath->subpaths) {
            Path *subpath = (Path *) lfirst(l);

            if (i == 0)
                apath->path.startup_cost = subpath->startup_cost;
            else if (i < apath->path.parallel_workers)
                apath->path.startup_cost = Min(apath->path.startup_cost,
                                             subpath->startup_cost);

            // Apply parallel divisor scaling
            if (i < apath->first_partial_path)
                apath->path.rows += subpath->rows / parallel_divisor;
            else {
                double subpath_parallel_divisor = get_parallel_divisor(subpath);
                apath->path.rows += subpath->rows * (subpath_parallel_divisor / parallel_divisor);
                apath->path.total_cost += subpath->total_cost;
            }

            apath->path.rows = clamp_row_est(apath->path.rows);
            i++;
        }

        // Add cost for non-partial subpaths
        apath->path.total_cost += append_nonpartial_cost(apath->subpaths,
                                                        apath->first_partial_path,
                                                        apath->path.parallel_workers);
    }

    // Add per-tuple overhead
    apath->path.total_cost += cpu_tuple_cost * APPEND_CPU_COST_MULTIPLIER * apath->path.rows;
}
```