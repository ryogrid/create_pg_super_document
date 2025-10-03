# get_cheapest_fractional_path_for_pathkeys

## Location
[src/backend/optimizer/path/pathkeys.c:664-696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L664-L696)

## Overview
Finds the cheapest path for retrieving a specified fraction of all tuples that satisfies given pathkeys and parameterization requirements.

## Definition

```c
Path *
get_cheapest_fractional_path_for_pathkeys(List *paths,
										  List *pathkeys,
										  Relids required_outer,
										  double fraction)
```
## Detailed Description
This function is a specialized variant of path selection that optimizes for scenarios where only a fraction of the total result set will be retrieved. It is particularly useful for queries with LIMIT clauses or other operations that terminate early. The function uses fractional path cost comparison instead of total cost comparison, which takes into account that startup costs become more significant when only retrieving partial results.

Like its counterpart get_cheapest_path_for_pathkeys, it iterates through candidate paths, prioritizing cost comparison over pathkey comparison for efficiency, then validates that the path satisfies the required ordering (pathkeys) and parameterization constraints.

## Parameters / Member Variables
- `*paths`: List of possible paths that all generate the same relation
- `*pathkeys`: Required ordering in canonical form that the selected path must satisfy
- `required_outer`: Allowable outer relations for parameterized paths
- `fraction`: The fraction of total tuples expected to be retrieved (between 0.0 and 1.0)
## Dependencies
- Functions called/Symbols referenced:
  - [compare_fractional_path_costs](../c/compare_fractional_path_costs.md)
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - PATH_REQ_OUTER
- Called from (representative examples):
  - [generate_orderedappend_paths](generate_orderedappend_paths.md)
  - [build_minmax_path](../b/build_minmax_path.md)

## Notes and Other Information
- The fraction parameter is interpreted by compare_fractional_path_costs() to weight startup vs. total costs appropriately
- Returns NULL if no suitable path is found
- Optimized for queries that will terminate early (e.g., with LIMIT clauses)
- Cost comparison is performed before pathkey comparison as an optimization
- Part of PostgreSQL's fractional path cost optimization framework for partial result retrieval

## Simplified Source

```c
Path *get_cheapest_fractional_path_for_pathkeys(List *paths, List *pathkeys,
                                               Relids required_outer, double fraction) {
    Path *best_path = NULL;

    // Find cheapest path that meets requirements
    foreach(cell, paths) {
        Path *path = (Path *) lfirst(cell);

        // Skip if more expensive than current best
        if (best_path != NULL &&
            compare_fractional_path_costs(best_path, path, fraction) <= 0)
            continue;

        // Check if path satisfies ordering and parameterization
        if (pathkeys_contained_in(pathkeys, path->pathkeys) &&
            bms_is_subset(PATH_REQ_OUTER(path), required_outer)) {
            best_path = path;
        }
    }

    return best_path;
}
```