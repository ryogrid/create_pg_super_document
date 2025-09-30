# create_projection_path

## Location
[src/backend/optimizer/util/pathnode.c:2685-2792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L2685-L2792)

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
  - [is_projection_capable_path](../i/is_projection_capable_path.md)
  - [equal](../e/equal.md)
- Called from (representative examples):
  - [adjust_paths_for_srfs](../a/adjust_paths_for_srfs.md)
  - [apply_scanjoin_target_to_paths](../a/apply_scanjoin_target_to_paths.md)
  - [apply_projection_to_path](../a/apply_projection_to_path.md)

## Notes and Other Information
The function implements a key optimization by setting dummypp flag when no separate Result node is needed. This occurs when the subpath can project directly or when target expressions match the input. Projection operations preserve the sort order (pathkeys) from the input path. Cost calculation accounts for expression evaluation overhead and potential cpu_tuple_cost when a Result node is required.

## Simplified Source
```c
ProjectionPath *
create_projection_path(PlannerInfo *root,
                      RelOptInfo *rel,
                      Path *subpath,
                      PathTarget *target)
{
    ProjectionPath *pathnode = makeNode(ProjectionPath);
    PathTarget *oldtarget;

    // Unwrap nested ProjectionPaths to avoid stacking
    if (IsA(subpath, ProjectionPath))
    {
        ProjectionPath *subpp = (ProjectionPath *) subpath;
        subpath = subpp->subpath;
    }

    // Initialize basic path properties
    pathnode->path.pathtype = T_Result;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = target;
    pathnode->path.param_info = NULL;
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel &&
        subpath->parallel_safe &&
        is_parallel_safe(root, (Node *) target->exprs);
    pathnode->path.parallel_workers = subpath->parallel_workers;
    pathnode->path.pathkeys = subpath->pathkeys;  // Preserve sort order

    pathnode->subpath = subpath;

    // Check if we can optimize away the Result node
    oldtarget = subpath->pathtarget;
    if (is_projection_capable_path(subpath) ||
        equal(oldtarget->exprs, target->exprs))
    {
        // No separate Result node needed
        pathnode->dummypp = true;
        pathnode->path.rows = subpath->rows;
        pathnode->path.startup_cost = subpath->startup_cost +
            (target->cost.startup - oldtarget->cost.startup);
        pathnode->path.total_cost = subpath->total_cost +
            (target->cost.startup - oldtarget->cost.startup) +
            (target->cost.per_tuple - oldtarget->cost.per_tuple) * subpath->rows;
    }
    else
    {
        // Need a separate Result node
        pathnode->dummypp = false;
        pathnode->path.rows = subpath->rows;
        pathnode->path.startup_cost = subpath->startup_cost +
            target->cost.startup;
        pathnode->path.total_cost = subpath->total_cost +
            target->cost.startup +
            (cpu_tuple_cost + target->cost.per_tuple) * subpath->rows;
    }

    return pathnode;
}
```