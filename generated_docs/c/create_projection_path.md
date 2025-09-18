# create_projection_path

## Location
src/backend/optimizer/util/pathnode.c: 2685 - 2792

## Overview
Creates a pathnode that represents performing a projection operation, potentially optimizing away unnecessary Result nodes when the underlying path can handle projection directly.

## Definition
```c
ProjectionPath *create_projection_path(PlannerInfo *root,
                                      RelOptInfo *rel,
                                      Path *subpath,
                                      PathTarget *target)
```

## Detailed Description
This function constructs a ProjectionPath node that represents computing a specific target list (projection) from input data. It implements an optimization where if the underlying path node can perform projection itself, or if the desired target matches what would be produced anyway, no separate Result node is needed (dummypp = true). The function also prevents stacking ProjectionPath nodes by automatically unwrapping nested ProjectionPaths. Cost calculation differs based on whether a separate Result node is required.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning information
- `rel`: RelOptInfo representing the parent relation for the result
- `subpath`: Path representing the source of input data
- `target`: PathTarget specifying the desired output columns and expressions

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - IsA
  - [is_parallel_safe](../i/is_parallel_safe.md)
  - is_projection_capable_path
  - [equal](../e/equal.md)
- Called from (representative examples):
  - [adjust_paths_for_srfs](../a/adjust_paths_for_srfs.md)
  - [apply_scanjoin_target_to_paths](../a/apply_scanjoin_target_to_paths.md)
  - [apply_projection_to_path](../a/apply_projection_to_path.md)

## Notes and Other Information
The function implements a key optimization by setting dummypp flag when no separate Result node is needed. This occurs when the subpath can project directly or when target expressions match the input. Projection operations preserve the sort order (pathkeys) from the input path. Cost calculation accounts for expression evaluation overhead and potential cpu_tuple_cost when a Result node is required.