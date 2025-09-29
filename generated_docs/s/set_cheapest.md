# set_cheapest

## Location
[src/backend/optimizer/util/pathnode.c:242-419](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L242-L419)

## Overview
Analyzes all paths for a relation and identifies the minimum-cost paths for startup cost and total cost, storing them in the relation's cheapest-path fields.

## Definition

```c
structure of a Path,
 *	  since much of it may be shared with other Paths or the query tree itself;
```
## Detailed Description
This function is a critical component of PostgreSQL's cost-based optimizer that processes all paths for a given relation and identifies the most cost-effective options. It maintains several categories of cheapest paths:

1. **cheapest_startup_path**: The unparameterized path with lowest startup cost
2. **cheapest_total_path**: The unparameterized path with lowest total cost (or best parameterized path if no unparameterized paths exist)  
3. **cheapest_parameterized_paths**: A list of all surviving parameterized paths plus the cheapest unparameterized path

For parameterized paths, the function uses a sophisticated comparison that considers both cost and the degree of parameterization (fewer required outer relations is better). When paths have identical costs, it prefers paths with better sort orderings using pathkey comparison.

## Parameters / Member Variables
- : RelOptInfo structure containing the pathlist to analyze and fields to update with cheapest paths

## Dependencies
- Functions called/Symbols referenced:
  - [bms_subset_compare](../b/bms_subset_compare.md)
  - PATH_REQ_OUTER
  - [compare_path_costs](../c/compare_path_costs.md)
  - [compare_pathkeys](../c/compare_pathkeys.md)
  - [lcons](../l/lcons.md)
  - BMS_EQUAL, BMS_SUBSET1, BMS_SUBSET2, BMS_DIFFERENT (enum values)
  - STARTUP_COST, TOTAL_COST (enum values)
  - PATHKEYS_BETTER2 (enum value)
- Called from (representative examples):
  - [set_rel_pathlist](set_rel_pathlist.md)
  - [set_dummy_rel_pathlist](set_dummy_rel_pathlist.md)
  - [standard_join_search](standard_join_search.md)
  - [generate_partitionwise_join_paths](../g/generate_partitionwise_join_paths.md)
  - [query_planner](../q/query_planner.md)
  - [subquery_planner](subquery_planner.md)
  - [create_grouping_paths](../c/create_grouping_paths.md)
  - [create_window_paths](../c/create_window_paths.md)
  - [create_distinct_paths](../c/create_distinct_paths.md)

## Notes and Other Information
This function is typically called after all paths for a relation have been constructed and added via add_path(). It ensures that the optimizer has easy access to the most cost-effective execution options without having to search through the entire pathlist repeatedly. The function handles both unparameterized and parameterized paths, with parameterized paths requiring outer relation values to execute.

## Simplified Source

```c
void set_cheapest(RelOptInfo *parent_rel) {
    Path *cheapest_startup_path = NULL;
    Path *cheapest_total_path = NULL;
    Path *best_param_path = NULL;
    List *parameterized_paths = NIL;

    // Error if no paths available
    if (parent_rel->pathlist == NIL)
        elog(ERROR, "could not devise a query plan for the given query");

    // Examine each path in the pathlist
    foreach(p, parent_rel->pathlist) {
        Path *path = (Path *) lfirst(p);

        if (path->param_info) {
            // Handle parameterized path
            parameterized_paths = lappend(parameterized_paths, path);

            // Skip further parameterized analysis if we have unparameterized cheapest
            if (cheapest_total_path)
                continue;

            // Track best parameterized path (least parameterized with lowest cost)
            if (best_param_path == NULL) {
                best_param_path = path;
            } else {
                // Compare parameterization levels and costs
                switch (bms_subset_compare(PATH_REQ_OUTER(path),
                                         PATH_REQ_OUTER(best_param_path))) {
                    case BMS_EQUAL:
                        if (compare_path_costs(path, best_param_path, TOTAL_COST) < 0)
                            best_param_path = path;
                        break;
                    case BMS_SUBSET1:
                        best_param_path = path; // Less parameterized
                        break;
                    // Keep existing path for BMS_SUBSET2 and BMS_DIFFERENT
                }
            }
        } else {
            // Handle unparameterized path
            if (cheapest_total_path == NULL) {
                cheapest_startup_path = cheapest_total_path = path;
                continue;
            }

            // Compare startup costs, preferring better pathkeys on ties
            int cmp = compare_path_costs(cheapest_startup_path, path, STARTUP_COST);
            if (cmp > 0 || (cmp == 0 &&
                compare_pathkeys(cheapest_startup_path->pathkeys, path->pathkeys) == PATHKEYS_BETTER2))
                cheapest_startup_path = path;

            // Compare total costs, preferring better pathkeys on ties
            cmp = compare_path_costs(cheapest_total_path, path, TOTAL_COST);
            if (cmp > 0 || (cmp == 0 &&
                compare_pathkeys(cheapest_total_path->pathkeys, path->pathkeys) == PATHKEYS_BETTER2))
                cheapest_total_path = path;
        }
    }

    // Include cheapest unparameterized path in parameterized list
    if (cheapest_total_path)
        parameterized_paths = lcons(cheapest_total_path, parameterized_paths);

    // Use best parameterized path as fallback if no unparameterized path
    if (cheapest_total_path == NULL)
        cheapest_total_path = best_param_path;

    // Set the relation's cheapest path fields
    parent_rel->cheapest_startup_path = cheapest_startup_path;
    parent_rel->cheapest_total_path = cheapest_total_path;
    parent_rel->cheapest_unique_path = NULL; // Computed only if needed
    parent_rel->cheapest_parameterized_paths = parameterized_paths;
}
```