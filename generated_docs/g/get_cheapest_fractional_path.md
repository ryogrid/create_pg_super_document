# get_cheapest_fractional_path

## Location
[src/backend/optimizer/plan/planner.c:6499-6541](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L6499-L6541)

## Overview
Finds the cheapest path for retrieving a specified fraction of all tuples expected to be returned by a given relation.

## Definition
```c
Path *
get_cheapest_fractional_path(RelOptInfo *rel, double tuple_fraction)
```

## Detailed Description
This function selects the optimal path from a relation's pathlist based on the cost of retrieving only a fraction of the total tuples, rather than all tuples. This is particularly useful for queries with LIMIT clauses or when only partial results are needed.

The function interprets tuple_fraction the same way as grouping_planner and assumes that set_cheapest() has already been run on the given relation. It starts with the cheapest total path as the baseline and then examines all paths in the relation's pathlist to find one that might be cheaper for the specified fraction.

**Algorithm:**
1. If tuple_fraction <= 0.0, returns the cheapest total path (all tuples needed)
2. Converts absolute tuple counts to fractions if tuple_fraction >= 1.0
3. Iterates through all paths, using compare_fractional_path_costs() to determine if any path is cheaper for the given fraction
4. Returns the path with the lowest fractional cost

This is essential for optimizing queries where only a portion of results will be consumed, as some paths (like index scans) may be more efficient for small fractions even if they're more expensive overall.

## Parameters / Member Variables
- `rel`: RelOptInfo structure containing the relation's optimization information and available paths
- `tuple_fraction`: The fraction of tuples expected to be retrieved (can be absolute count if >= 1.0)

## Dependencies
- Functions called/Symbols referenced:
  - [compare_fractional_path_costs](../c/compare_fractional_path_costs.md)
- Called from:
  - [standard_planner](../s/standard_planner.md) (src/backend/optimizer/plan/planner.c:420)
  - [make_subplan](../m/make_subplan.md) (src/backend/optimizer/plan/subselect.c:233)
- Declared in:
  - PLANNER_H (src/include/optimizer/planner.h:57)

## Notes and Other Information
- This is a public function (not static) available to other optimizer modules
- The function assumes set_cheapest() has been run on the relation beforehand
- Tuple fraction conversion allows both fractional (0.0-1.0) and absolute count inputs
- The function always considers the cheapest_total_path as a baseline candidate
- Essential for LIMIT optimization and partial result scenarios
- Works by comparing fractional costs rather than total costs for better path selection