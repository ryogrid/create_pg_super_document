# compare_fractional_path_costs

## Location
[src/backend/optimizer/util/pathnode.c:115-163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L115-L163)

## Overview
Compares the costs of two paths for fetching a specified fraction of total tuples, using interpolated costs between startup and total cost.

## Definition

```c
int
compare_fractional_path_costs(Path *path1, Path *path2,
							  double fraction)
```
## Detailed Description
This function extends the basic path cost comparison to handle partial result sets. It calculates the interpolated cost for fetching a given fraction of tuples from each path, where the cost is computed as:

cost = startup_cost + fraction * (total_cost - startup_cost)

This allows the optimizer to choose the best path when only a portion of the result set will be fetched, which is common in queries with LIMIT clauses or when paths are used as inner inputs to nested loop joins.

When fraction is <= 0 or >= 1, the function delegates to compare_path_costs with TOTAL_COST criterion.

## Parameters / Member Variables
- : First path to compare
- : Second path to compare
- : Fraction of tuples to fetch (0.0 to 1.0)

## Dependencies
- Functions called/Symbols referenced:
  - Cost (type)
  - [compare_path_costs](compare_path_costs.md)
  - TOTAL_COST (enum value)
- Called from (representative examples):
  - [get_cheapest_fractional_path_for_pathkeys](../g/get_cheapest_fractional_path_for_pathkeys.md)
  - [get_cheapest_fractional_path](../g/get_cheapest_fractional_path.md)  
  - [choose_hashed_setop](choose_hashed_setop.md)

## Notes and Other Information
This function is crucial for optimizing queries with LIMIT clauses or when the query planner knows that only a fraction of the result will be consumed. The linear interpolation between startup and total cost provides a reasonable approximation of the actual cost for partial result fetching.

## Simplified Source

```c
int compare_fractional_path_costs(Path *path1, Path *path2, double fraction) {
    Cost cost1, cost2;

    // For full result set or invalid fraction, use total cost comparison
    if (fraction <= 0.0 || fraction >= 1.0) {
        return compare_path_costs(path1, path2, TOTAL_COST);
    }

    // Calculate interpolated costs for partial result fetch
    // cost = startup_cost + fraction * (total_cost - startup_cost)
    cost1 = path1->startup_cost + fraction * (path1->total_cost - path1->startup_cost);
    cost2 = path2->startup_cost + fraction * (path2->total_cost - path2->startup_cost);

    // Return comparison result: -1 (path1 cheaper), 0 (equal), +1 (path2 cheaper)
    if (cost1 < cost2) return -1;
    if (cost1 > cost2) return +1;
    return 0;
}
```