# create_merge_append_path

## Location
[src/backend/optimizer/util/pathnode.c:1415-1517](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L1415-L1517)

## Overview
Creates a path node corresponding to a MergeAppend plan, which merges multiple pre-sorted input streams to produce a single sorted output stream.

## Definition

```c
MergeAppendPath *
create_merge_append_path(PlannerInfo *root,
						 RelOptInfo *rel,
						 List *subpaths,
						 List *pathkeys,
						 Relids required_outer)
```
## Detailed Description
This function constructs a MergeAppendPath node that represents a MergeAppend operation in PostgreSQL's query execution plan. Unlike a regular Append which simply concatenates results, MergeAppend merges multiple already-sorted input streams to maintain the sort order in the output. The function calculates costs by considering whether each subpath is already adequately sorted or requires an additional Sort node.

For subpaths that are not properly sorted, the function includes the cost of inserting a Sort node. When there's only one child path with matching parallel awareness, the operation becomes a no-op and inherits the child's costs directly. The function handles the application of query-wide LIMIT when appropriate.

## Parameters / Member Variables
- `*root`: PlannerInfo context for the query being planned
- `*rel`: RelOptInfo for the relation this path represents
- `*subpaths`: List of child paths to be merged (must produce compatible sort orders)
- `*pathkeys`: Required sort ordering for the merged output
- `required_outer`: Set of outer relids required by this path
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (MergeAppendPath creation)
  - [get_appendrel_parampathinfo](../g/get_appendrel_parampathinfo.md)
  - [bms_equal](../b/bms_equal.md)
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md)
  - [cost_sort](cost_sort.md)
  - [cost_merge_append](cost_merge_append.md)
  - PATH_REQ_OUTER
- Called from (representative examples):
  - [generate_orderedappend_paths](../g/generate_orderedappend_paths.md)
  - [generate_union_paths](../g/generate_union_paths.md)

## Notes and Other Information
- [MergeAppend](../M/MergeAppend.md) is never parallel-aware (parallel_aware = false, parallel_workers = 0)
- All child paths must have the same parameterization (required_outer)
- For each subpath, checks if it's adequately ordered using pathkeys_contained_in()
- If a subpath needs sorting, includes the cost of a Sort node in the total calculation
- Single-child MergeAppend with matching parallel awareness becomes a no-op
- Applies query-wide LIMIT when the path represents the sole base relation
- The resulting path maintains the specified pathkeys ordering through the merge operation
- More expensive than regular Append but preserves sort order without a final sort step

## Simplified Source

```c
MergeAppendPath *create_merge_append_path(PlannerInfo *root, RelOptInfo *rel,
                                         List *subpaths, List *pathkeys,
                                         Relids required_outer) {
    MergeAppendPath *pathnode = makeNode(MergeAppendPath);
    Cost input_startup_cost = 0;
    Cost input_total_cost = 0;
    ListCell *l;

    // Initialize path properties
    pathnode->path.pathtype = T_MergeAppend;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;
    pathnode->path.param_info = get_appendrel_parampathinfo(rel, required_outer);
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel;
    pathnode->path.parallel_workers = 0;
    pathnode->path.pathkeys = pathkeys;
    pathnode->subpaths = subpaths;

    // Set limit tuples if applicable
    if (bms_equal(rel->relids, root->all_query_rels))
        pathnode->limit_tuples = root->limit_tuples;
    else
        pathnode->limit_tuples = -1.0;

    // Calculate input costs and row counts
    pathnode->path.rows = 0;
    foreach(l, subpaths) {
        Path *subpath = (Path *) lfirst(l);

        pathnode->path.rows += subpath->rows;
        pathnode->path.parallel_safe = pathnode->path.parallel_safe && subpath->parallel_safe;

        if (pathkeys_contained_in(pathkeys, subpath->pathkeys)) {
            // Subpath already properly sorted
            input_startup_cost += subpath->startup_cost;
            input_total_cost += subpath->total_cost;
        } else {
            // Need to add Sort node cost
            Path sort_path;
            cost_sort(&sort_path, root, pathkeys, subpath->total_cost,
                     subpath->rows, subpath->pathtarget->width, 0.0,
                     work_mem, pathnode->limit_tuples);
            input_startup_cost += sort_path.startup_cost;
            input_total_cost += sort_path.total_cost;
        }
    }

    // Calculate final MergeAppend costs
    if (list_length(subpaths) == 1 &&
        ((Path *) linitial(subpaths))->parallel_aware == pathnode->path.parallel_aware) {
        // Single-child MergeAppend is a no-op
        pathnode->path.startup_cost = input_startup_cost;
        pathnode->path.total_cost = input_total_cost;
    } else {
        // Normal MergeAppend cost calculation
        cost_merge_append(&pathnode->path, root, pathkeys, list_length(subpaths),
                         input_startup_cost, input_total_cost, pathnode->path.rows);
    }

    return pathnode;
}
```