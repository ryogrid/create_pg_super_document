# create_sort_path

## Location
[src/backend/optimizer/util/pathnode.c:3000-3043](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L3000-L3043)

## Overview
Creates a pathnode that represents performing an explicit sort operation on input data to achieve a desired sort order.

## Definition
```c
SortPath *create_sort_path(PlannerInfo *root,
                          RelOptInfo *rel,
                          Path *subpath,
                          List *pathkeys,
                          double limit_tuples)
```

## Detailed Description
This function creates a SortPath node that represents a complete explicit sort operation. Unlike incremental sort, this performs a full sort on all input data regardless of any existing ordering. The function initializes all standard Path node fields, sets up the pathnode structure to represent the sort operation, and calculates the associated costs using the `cost_sort` function. This is the fundamental sorting path used when no pre-existing order can be exploited.

The sort operation does not project or transform the data - it simply reorders the input rows according to the specified pathkeys while preserving the original pathtarget structure.

## Parameters / Member Variables
- `root`: PlannerInfo containing planning context, statistics, and optimizer settings
- `rel`: RelOptInfo representing the parent relation associated with the result
- `subpath`: Path representing the source of input data to be sorted
- `pathkeys`: List representing the desired sort order specification
- `limit_tuples`: Estimated bound on number of output tuples, or -1 if no LIMIT or estimate unavailable

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates SortPath node)
  - [cost_sort](cost_sort.md) (calculates full sorting costs)
  - SortPath (return type structure)
- Called from (representative examples):
  - [generate_useful_gather_paths](../g/generate_useful_gather_paths.md)
  - [create_one_window_path](create_one_window_path.md)
  - [create_partial_distinct_paths](create_partial_distinct_paths.md)
  - [create_final_distinct_paths](create_final_distinct_paths.md)
  - [create_ordered_paths](create_ordered_paths.md)
  - [make_ordered_path](../m/make_ordered_path.md)
  - [gather_grouping_paths](../g/gather_grouping_paths.md)

## Notes and Other Information
- This is used when no existing order can be leveraged, requiring a complete sort of all input data
- The function assumes operation above joins (no parameterization) and inherits parallel safety from subpath
- Cost calculation considers work_mem setting for determining sort strategy (memory vs disk-based)
- Comparison cost is currently hardcoded to 0.0 with a TODO comment indicating this may need improvement
- The pathtype is set to T_Sort to distinguish it from incremental sort (T_IncrementalSort)
- Sort operations are often expensive but necessary for ORDER BY, GROUP BY, and other operations requiring ordered data