# apply_projection_to_path

## Location
[src/backend/optimizer/util/pathnode.c:2793-2881](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L2793-L2881)

## Overview
Adds a projection step to a path or directly applies the target to the given path when possible, providing a more invasive in-place alternative to create_projection_path.

## Definition
```c
Path *apply_projection_to_path(PlannerInfo *root,
                              RelOptInfo *rel,
                              Path *path,
                              PathTarget *target)
```

## Detailed Description
This function applies a projection target to a path, with the key difference from create_projection_path being that it modifies the input path in-place when possible. If the path cannot handle projection directly, it falls back to create_projection_path. For parallel paths (GatherPath/GatherMergePath), it attempts to push the projection down to worker processes when the target expressions are parallel-safe. The function handles cost adjustments and parallel safety considerations appropriately.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning information
- `rel`: RelOptInfo representing the parent relation for the result
- `path`: Path to be modified in-place with the new projection
- `target`: PathTarget specifying the desired output columns and expressions

## Dependencies
- Functions called/Symbols referenced:
  - [is_projection_capable_path](../i/is_projection_capable_path.md)
  - [create_projection_path](../c/create_projection_path.md)
  - [is_parallel_safe](../i/is_parallel_safe.md)
  - IsA
- Called from (representative examples):
  - [build_minmax_path](../b/build_minmax_path.md)
  - [create_ordered_paths](../c/create_ordered_paths.md)
  - [adjust_paths_for_srfs](adjust_paths_for_srfs.md)

## Notes and Other Information
This function is more invasive than create_projection_path as it modifies the input path in-place, so it should only be used when the caller knows the path isn't referenced elsewhere. For parallel paths, it creates separate projection paths for subpaths to enable worker participation in projection. The function carefully manages parallel safety flags when non-parallel-safe expressions are introduced.