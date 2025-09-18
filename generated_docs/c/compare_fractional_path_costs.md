# compare_fractional_path_costs

## Location
src/backend/optimizer/util/pathnode.c: 115 - 163

## Overview
Compares the costs of two paths for fetching a specified fraction of total tuples, using interpolated costs between startup and total cost.

## Definition


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