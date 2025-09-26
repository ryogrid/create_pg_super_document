# IncrementalSortPath

## Location
src/include/nodes/pathnodes.h: 2211 - 2215

## Overview
IncrementalSortPath represents an incremental sort operation that optimizes sorting performance when the input data is already partially sorted on leading key columns.

## Definition
```c
typedef struct IncrementalSortPath
{
    SortPath    spath;
    int         nPresortedCols; /* number of presorted columns */
} IncrementalSortPath;
```

## Detailed Description
IncrementalSortPath extends SortPath to handle cases where the input data is already sorted on some leading columns of the desired sort order. Instead of performing a full sort on all data, incremental sort groups the input by the presorted columns and sorts each group independently on the remaining columns. This can significantly reduce memory usage and improve performance.

For example, if data is already sorted by (a, b) and we need to sort by (a, b, c), incremental sort will process groups of rows with the same (a, b) values and sort each group only by column c. This approach reduces the working set size and can often avoid spilling to disk.

The optimization is particularly effective when the presorted columns have high cardinality, creating many small groups that can be sorted efficiently in memory.

## Parameters / Member Variables
- `spath`: Base SortPath structure containing the path information, subpath, and inherited Path details including pathkeys and cost estimates
- `nPresortedCols`: Number of leading columns that are already sorted in the input data, determining how the incremental sort will group and process the data

## Dependencies
- Functions called/Symbols referenced:
  - SortPath (inherited base structure)
- Called from (representative examples):
  - create_incremental_sort_path (path creation)
  - create_incrementalsort_plan (plan generation)
  - create_set_projection_path (optimization integration)

## Notes and Other Information
- IncrementalSortPath inherits from SortPath, gaining all the base sorting functionality while adding the incremental optimization
- The nPresortedCols value must be less than the total number of sort keys for incremental sort to be beneficial
- Most effective when presorted columns have high cardinality, creating many small groups to sort independently
- Can dramatically reduce memory requirements compared to full sorting, often preventing spills to disk
- The planner's cost model considers both the grouping overhead and the reduced sorting cost when evaluating incremental sort paths
- Introduced as an optimization for cases where indexes or previous operations provide partial ordering
- The execution engine processes data in groups, maintaining the presorted order while sorting within each group