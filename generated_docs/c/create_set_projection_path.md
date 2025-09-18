# create_set_projection_path

## Location
src/backend/optimizer/util/pathnode.c: 2882 - 2950

## Overview
Creates a pathnode that represents performing a projection containing set-returning functions (SRFs), which can produce multiple output rows per input row.

## Definition
```c
ProjectSetPath *create_set_projection_path(PlannerInfo *root,
                                          RelOptInfo *rel,
                                          Path *subpath,
                                          PathTarget *target)
```

## Detailed Description
This function constructs a ProjectSetPath node specifically designed to handle projections that include set-returning functions. Unlike regular projections, SRFs can generate multiple output rows from a single input row, requiring special handling for row count estimation and cost calculation. The function determines the maximum number of rows any SRF will produce and uses this for cardinality estimation. Cost calculation includes additional overhead for processing the expanded result set.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning information
- `rel`: RelOptInfo representing the parent relation for the result
- `subpath`: Path representing the source of input data
- `target`: PathTarget containing expressions including set-returning functions

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [is_parallel_safe](../i/is_parallel_safe.md)
  - [expression_returns_set_rows](../e/expression_returns_set_rows.md)
  - lfirst
- Called from (representative examples):
  - [adjust_paths_for_srfs](../a/adjust_paths_for_srfs.md)

## Notes and Other Information
The function estimates output cardinality by finding the maximum number of rows returned by any SRF in the target list. Cost calculation uses a heuristic from PostgreSQL 9.6: cpu_tuple_cost per input row plus half cpu_tuple_cost for each additional output row. The pathkeys are preserved from the subpath, though this may need revisiting. Like other projection paths, it assumes no parameterization above joins.