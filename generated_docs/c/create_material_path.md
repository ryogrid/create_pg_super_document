# create_material_path

## Location
[src/backend/optimizer/util/pathnode.c:1566-1597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L1566-L1597)

## Overview
Creates a MaterialPath node that represents a Material plan operation, which materializes (stores) the output of a subpath to enable efficient re-reading of the data.

## Definition

```c
MaterialPath *
create_material_path(RelOptInfo *rel, Path *subpath)
```
## Detailed Description
The  function constructs a MaterialPath node that corresponds to a Material plan node in PostgreSQL's query execution. A Material node is used to materialize the output of its subpath, storing the result tuples in memory or on disk so they can be read multiple times efficiently. This is particularly useful when the same data needs to be accessed repeatedly, such as in certain join algorithms or when multiple scans of the same intermediate result are required.

The function initializes all necessary fields of the MaterialPath structure, copies relevant properties from the subpath, and calculates the cost using the  function. The resulting path maintains the same ordering (pathkeys) as its subpath and inherits parallelization properties appropriately.

## Parameters / Member Variables
- `*rel`: The RelOptInfo representing the relation that this material path will produce
- `*subpath`: The input Path whose output will be materialized
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the MaterialPath node)
  - [cost_material](cost_material.md) (to calculate the cost of materialization)
  - [MaterialPath](../M/MaterialPath.md) (the path node type being created)
- Called from (representative examples):
  - [set_tablesample_rel_pathlist](../s/set_tablesample_rel_pathlist.md) (for table sampling operations)
  - [match_unsorted_outer](../m/match_unsorted_outer.md) (in join path planning)
  - [reparameterize_path](../r/reparameterize_path.md) (when reparameterizing paths)

## Notes and Other Information
- The function asserts that the subpath's parent matches the provided rel parameter
- The material path is never parallel_aware itself, but it can be parallel_safe if both the relation and subpath are parallel-safe
- The parallel_workers count is inherited from the subpath
- The pathkeys (sort ordering) are preserved from the subpath since materialization doesn't change the order
- Cost calculation includes both the cost of reading the subpath and the overhead of materializing the results

## Simplified Source

```c
MaterialPath *
create_material_path(RelOptInfo *rel, Path *subpath)
{
    MaterialPath *pathnode = makeNode(MaterialPath);

    Assert(subpath->parent == rel);

    // Initialize basic path properties
    pathnode->path.pathtype = T_Material;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;
    pathnode->path.param_info = subpath->param_info;

    // Set parallel execution properties
    pathnode->path.parallel_aware = false;  // Material is never parallel_aware
    pathnode->path.parallel_safe = rel->consider_parallel && subpath->parallel_safe;
    pathnode->path.parallel_workers = subpath->parallel_workers;

    // Preserve ordering from subpath
    pathnode->path.pathkeys = subpath->pathkeys;

    // Store reference to input path
    pathnode->subpath = subpath;

    // Calculate materialization costs
    cost_material(&pathnode->path,
                  subpath->startup_cost,
                  subpath->total_cost,
                  subpath->rows,
                  subpath->pathtarget->width);

    return pathnode;
}
```