# get_cheapest_path_for_pathkeys

## Location
[src/backend/optimizer/path/pathkeys.c:618-663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L618-L663)

## Overview
Finds the cheapest path (according to specified cost criterion) that satisfies given pathkeys and parameterization requirements, with optional parallel-safety constraints.

## Definition

```c
Path *
get_cheapest_path_for_pathkeys(List *paths, List *pathkeys,
							   Relids required_outer,
							   CostSelector cost_criterion,
							   bool require_parallel_safe)
```
## Detailed Description
This function is a core component of PostgreSQL's query optimizer that searches through a list of candidate paths to find the one that best matches the specified ordering requirements (pathkeys) while minimizing cost. It performs cost-based optimization by comparing paths using either startup cost or total cost as the selection criterion. The function also handles parameterized paths and can optionally restrict selection to parallel-safe paths only.

The algorithm iterates through all provided paths, filtering based on parallel-safety requirements if specified, then performs cost comparison (which is cheaper than pathkey comparison) before checking if the path's pathkeys contain the required pathkeys and if the path's outer relation requirements are satisfied.

## Parameters / Member Variables
- : List of possible paths that all generate the same relation
- : Required ordering in canonical form that the selected path must satisfy
- : Allowable outer relations for parameterized paths
- : Cost selection criterion (STARTUP_COST or TOTAL_COST)
- : When true, only considers parallel-safe paths

## Dependencies
- Functions called/Symbols referenced:
  - [compare_path_costs](../c/compare_path_costs.md)
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - PATH_REQ_OUTER
  - CostSelector (type)
- Called from (representative examples):
  - [generate_orderedappend_paths](generate_orderedappend_paths.md)
  - [get_cheapest_parameterized_child_path](get_cheapest_parameterized_child_path.md)
  - [generate_mergejoin_paths](generate_mergejoin_paths.md)
  - [generate_union_paths](generate_union_paths.md)

## Notes and Other Information
- Cost comparison is performed before pathkey comparison as an optimization, since cost comparison is computationally cheaper
- Returns NULL if no suitable path is found
- The function assumes pathkeys are in canonical form
- Part of the pathkey-based optimization infrastructure in PostgreSQL's query planner

## Simplified Source

```c
Path *get_cheapest_path_for_pathkeys(List *paths, List *pathkeys,
                                    Relids required_outer,
                                    CostSelector cost_criterion,
                                    bool require_parallel_safe) {
    Path *matched_path = NULL;
    ListCell *l;

    foreach(l, paths) {
        Path *path = (Path *) lfirst(l);

        // Skip non-parallel-safe paths if required
        if (require_parallel_safe && !path->parallel_safe)
            continue;

        // Skip if more expensive than current best (cost comparison is cheaper than pathkey comparison)
        if (matched_path != NULL &&
            compare_path_costs(matched_path, path, cost_criterion) <= 0)
            continue;

        // Check if path satisfies pathkeys and parameterization requirements
        if (pathkeys_contained_in(pathkeys, path->pathkeys) &&
            bms_is_subset(PATH_REQ_OUTER(path), required_outer))
            matched_path = path;
    }

    return matched_path;
}
```