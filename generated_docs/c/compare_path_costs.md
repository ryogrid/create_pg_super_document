# compare_path_costs

## Location
[src/backend/optimizer/util/pathnode.c:69-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L69-L114)

## Overview
Compares the costs of two paths and returns an integer indicating which path is cheaper based on either startup cost or total cost criteria.

## Definition

```c
int
compare_path_costs(Path *path1, Path *path2, CostSelector criterion)
```
## Detailed Description
This function compares two execution paths by their costs and returns a standard comparison result (-1, 0, +1). The comparison criterion determines whether to prioritize startup cost or total cost. When the primary cost values are equal, the function uses the secondary cost as a tiebreaker to ensure a deterministic ordering.

For STARTUP_COST criterion:
- Primary comparison: startup_cost
- Tiebreaker: total_cost

For TOTAL_COST criterion (default):
- Primary comparison: total_cost  
- Tiebreaker: startup_cost

## Parameters / Member Variables
- : First path to compare
- : Second path to compare  
- : Cost selection criterion (STARTUP_COST or TOTAL_COST)

## Dependencies
- Functions called/Symbols referenced:
  - CostSelector (enum type)
  - STARTUP_COST (enum value)
- Called from (representative examples):
  - [get_cheapest_parameterized_child_path](../g/get_cheapest_parameterized_child_path.md)
  - [generate_mergejoin_paths](../g/generate_mergejoin_paths.md)
  - [get_cheapest_path_for_pathkeys](../g/get_cheapest_path_for_pathkeys.md)
  - [compare_fractional_path_costs](compare_fractional_path_costs.md)
  - [set_cheapest](../s/set_cheapest.md)
  - [append_total_cost_compare](../a/append_total_cost_compare.md)
  - [append_startup_cost_compare](../a/append_startup_cost_compare.md)

## Notes and Other Information
This is a fundamental utility function used throughout the PostgreSQL query optimizer for path selection and cost-based optimization. The deterministic tiebreaking ensures consistent behavior when paths have identical primary costs, which is important for reproducible query plans.