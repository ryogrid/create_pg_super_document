# create_gather_merge_path

## Location
[src/backend/optimizer/util/pathnode.c:1881-1945](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L1881-L1945)

## Overview
Creates a GatherMergePath node that represents collecting and merging sorted results from parallel workers, maintaining the sort order in the final output.

## Definition
```c
GatherMergePath *create_gather_merge_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
                                         PathTarget *target, List *pathkeys,
                                         Relids required_outer, double *rows)
```

## Detailed Description
The `create_gather_merge_path` function constructs a GatherMergePath node that corresponds to a GatherMerge plan node in PostgreSQL's query execution. A GatherMerge operation collects results from multiple parallel workers and merges them while preserving the sort order. This is particularly useful when you need sorted output from a parallel operation.

The function intelligently handles two scenarios:
1. **Pre-sorted input**: If the subpath is already adequately sorted according to the required pathkeys, no additional sorting is needed.
2. **Unsorted input**: If the subpath needs sorting, the function includes the cost of inserting a Sort node before the gather merge operation.

The function asserts that the subpath is parallel-safe and that pathkeys are provided, as these are prerequisites for a meaningful gather merge operation.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `rel`: The RelOptInfo representing the relation this gather merge path will produce
- `subpath`: The input Path that will be executed by parallel workers
- `target`: PathTarget specifying the desired output columns (uses rel->reltarget if NULL)
- `pathkeys`: List specifying the required sort order for the merged output
- `required_outer`: Relids representing required outer relations for parameterized paths
- `rows`: Pointer to the estimated number of output rows (may be modified)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the GatherMergePath node)
  - [get_baserel_parampathinfo](../g/get_baserel_parampathinfo.md) (to handle parameterized path information)
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md) (to check if subpath is adequately sorted)
  - [cost_sort](cost_sort.md) (to estimate sorting cost when needed)
  - [cost_gather_merge](cost_gather_merge.md) (to calculate the final cost of the gather merge operation)
  - [GatherMergePath](../G/GatherMergePath.md) (the path node type being created)
- Called from (representative examples):
  - [generate_gather_paths](../g/generate_gather_paths.md) (when generating parallel access paths)
  - [generate_useful_gather_paths](../g/generate_useful_gather_paths.md) (for useful parallel path generation)
  - [create_ordered_paths](create_ordered_paths.md) (when creating ordered execution paths)
  - [gather_grouping_paths](../g/gather_grouping_paths.md) (for parallel grouping operations)

## Notes and Other Information
- The function requires that the subpath is parallel_safe and that pathkeys are provided
- The path is never parallel_aware itself (it's the coordinator node)
- If the subpath doesn't match the required sort order, a Sort node cost is included
- The num_workers field is inherited from the subpath's parallel_workers setting
- Row count estimation adds the subpath's rows to the path's row count
- Parameter info is obtained using get_baserel_parampathinfo for proper parameterized path handling
- The final costing is delegated to cost_gather_merge which handles the complexity of merging multiple sorted streams

## Simplified Source

```c
GatherMergePath *
create_gather_merge_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
                         PathTarget *target, List *pathkeys,
                         Relids required_outer, double *rows)
{
    GatherMergePath *pathnode = makeNode(GatherMergePath);
    Cost input_startup_cost = 0;
    Cost input_total_cost = 0;

    Assert(subpath->parallel_safe);
    Assert(pathkeys);

    // Initialize basic path properties
    pathnode->path.pathtype = T_GatherMerge;
    pathnode->path.parent = rel;
    pathnode->path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
    pathnode->path.parallel_aware = false;

    // Set gather merge specific properties
    pathnode->subpath = subpath;
    pathnode->num_workers = subpath->parallel_workers;
    pathnode->path.pathkeys = pathkeys;
    pathnode->path.pathtarget = target ? target : rel->reltarget;
    pathnode->path.rows += subpath->rows;

    // Check if subpath is already sorted correctly
    if (pathkeys_contained_in(pathkeys, subpath->pathkeys)) {
        // No sorting needed - use subpath costs directly
        input_startup_cost += subpath->startup_cost;
        input_total_cost += subpath->total_cost;
    } else {
        // Need to sort first - include sorting cost
        Path sort_path;
        cost_sort(&sort_path, root, pathkeys, subpath->total_cost,
                  subpath->rows, subpath->pathtarget->width,
                  0.0, work_mem, -1);
        input_startup_cost += sort_path.startup_cost;
        input_total_cost += sort_path.total_cost;
    }

    // Calculate final gather merge cost
    cost_gather_merge(pathnode, root, rel, pathnode->path.param_info,
                      input_startup_cost, input_total_cost, rows);

    return pathnode;
}
```