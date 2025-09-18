# create_incremental_sort_path

## Location
src/backend/optimizer/util/pathnode.c: 2951 - 2999

## Overview
Creates a pathnode that represents performing an incremental sort, which is an optimization for sorting data that is already partially sorted by a prefix of the sort keys.

## Definition
```c
IncrementalSortPath *create_incremental_sort_path(PlannerInfo *root,
                                                 RelOptInfo *rel,
                                                 Path *subpath,
                                                 List *pathkeys,
                                                 int presorted_keys,
                                                 double limit_tuples)
```

## Detailed Description
This function creates an IncrementalSortPath node that represents an incremental sort operation. Incremental sorting is a PostgreSQL optimization that leverages existing partial ordering in the input data. When the input is already sorted by some prefix of the desired sort keys, incremental sorting can be significantly more efficient than a full sort because it only needs to sort within groups of rows that have the same values for the presorted columns.

The function initializes all the standard Path fields, sets up the pathnode structure, calculates the cost using `cost_incremental_sort`, and stores the number of presorted columns for later use during execution planning.

## Parameters / Member Variables
- `root`: PlannerInfo containing planning context and statistics
- `rel`: RelOptInfo representing the parent relation associated with the result
- `subpath`: Path representing the source of input data to be sorted
- `pathkeys`: List representing the desired complete sort order  
- `presorted_keys`: Number of leading sort keys by which the input path is already sorted
- `limit_tuples`: Estimated bound on number of output tuples, or -1 if no LIMIT or estimate unavailable

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates IncrementalSortPath node)
  - cost_incremental_sort (calculates sorting costs)
  - IncrementalSortPath (return type structure)
  - SortPath (embedded structure)
- Called from (representative examples):
  - generate_useful_gather_paths
  - create_one_window_path  
  - create_partial_distinct_paths
  - create_final_distinct_paths
  - create_ordered_paths
  - make_ordered_path

## Notes and Other Information
- Incremental sort is most beneficial when the input has a significant amount of pre-existing order that matches the required sort prefix
- The function assumes operation above joins (no parameterization) and inherits parallel safety from the subpath
- Cost calculation uses work_mem setting and considers the limited number of keys that need full sorting
- The comparison_cost parameter is currently set to 0.0 with a TODO comment suggesting this may need refinement