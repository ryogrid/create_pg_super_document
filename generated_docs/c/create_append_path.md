# create_append_path

## Location
[src/backend/optimizer/util/pathnode.c:1244-1374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L1244-L1374)

## Overview
Creates a path node corresponding to an Append plan, which combines results from multiple child paths either sequentially or in parallel.

## Definition

```c
struct a full ParamPathInfo
	 * for the path.  This supports building a Memoize path atop this path,
	 * and if this is a partitioned table the info may be useful for run-time
	 * pruning (cf make_partition_pruneinfo()).
	 *
	 * However, if we don't have "root" then that won't work and we fall back
	 * on the simpler get_appendrel_parampathinfo.  There's no point in doing
	 * the more expensive thing for a dummy path, either.
	 */
	if (rel->reloptkind == RELOPT_BASEREL && root && subpaths != NIL)
		pathnode->path.param_info = get_baserel_parampathinfo(root,
															  rel,
															  required_outer);
```
## Detailed Description
This function constructs an AppendPath node that represents an Append operation in PostgreSQL's query execution plan. The Append operation combines results from multiple input paths, which can be either regular subpaths or partial subpaths for parallel execution. The function handles various optimization scenarios including single-child optimization (where the Append becomes a no-op), parallel execution with cost-based sorting, and proper parameter propagation.

For parallel-aware append operations, the function sorts non-partial paths by descending total costs and partial paths by descending startup costs to minimize total execution time. When there's only one child path with matching parallel awareness, the function optimizes by inheriting the child's costs and pathkeys directly.

## Parameters / Member Variables
- : PlannerInfo context (can be NULL for some callers)
- : RelOptInfo for the relation this path represents
- : List of regular child paths to append
- : List of partial paths for parallel execution
- : Sort ordering required for the output (must be NIL for parallel-aware paths)
- : Set of outer relids required by this path
- : Number of parallel workers (must be > 0 if parallel_aware is true)
- : Whether this is a parallel-aware append operation
- : Optional row count override (use -1 to calculate from subpaths)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (AppendPath creation)
  - [get_baserel_parampathinfo](../g/get_baserel_parampathinfo.md)
  - [get_appendrel_parampathinfo](../g/get_appendrel_parampathinfo.md)
  - [list_sort](../l/list_sort.md)
  - [append_total_cost_compare](../a/append_total_cost_compare.md)
  - [append_startup_cost_compare](../a/append_startup_cost_compare.md)
  - [list_concat](../l/list_concat.md)
  - [bms_equal](../b/bms_equal.md)
  - [cost_append](cost_append.md)
  - PATH_REQ_OUTER
- Called from (representative examples):
  - [add_paths_to_append_rel](../a/add_paths_to_append_rel.md)
  - [generate_orderedappend_paths](../g/generate_orderedappend_paths.md)
  - [generate_union_paths](../g/generate_union_paths.md)
  - [set_dummy_rel_pathlist](../s/set_dummy_rel_pathlist.md)

## Notes and Other Information
- Handles the special case of NIL subpaths representing dummy access paths
- For baserels with root context, uses more comprehensive ParamPathInfo construction to support Memoize paths and runtime pruning
- Applies query-wide LIMIT when the path represents the sole base relation
- Single-child Append paths are optimized to inherit child properties when parallel awareness matches
- All child paths must have the same parameterization (required_outer)
- For parallel-aware appends, pathkeys must be NIL to allow cost-based sorting of subpaths

## Simplified Source

```c
AppendPath *
create_append_path(PlannerInfo *root, RelOptInfo *rel,
                  List *subpaths, List *partial_subpaths,
                  List *pathkeys, Relids required_outer,
                  int parallel_workers, bool parallel_aware,
                  double rows)
{
    AppendPath *pathnode = makeNode(AppendPath);

    // Initialize basic path properties
    pathnode->path.pathtype = T_Append;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;
    pathnode->path.parallel_aware = parallel_aware;
    pathnode->path.parallel_safe = rel->consider_parallel;
    pathnode->path.parallel_workers = parallel_workers;
    pathnode->path.pathkeys = pathkeys;

    // Set parameter info based on relation type
    if (rel->reloptkind == RELOPT_BASEREL && root && subpaths != NIL)
        pathnode->path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
    else
        pathnode->path.param_info = get_appendrel_parampathinfo(rel, required_outer);

    // For parallel append, sort paths for optimal execution
    if (parallel_aware)
    {
        // Sort non-partial paths by descending total cost
        list_sort(subpaths, append_total_cost_compare);
        // Sort partial paths by descending startup cost
        list_sort(partial_subpaths, append_startup_cost_compare);
    }

    // Combine subpaths and mark where partial paths begin
    pathnode->first_partial_path = list_length(subpaths);
    pathnode->subpaths = list_concat(subpaths, partial_subpaths);

    // Apply query-wide LIMIT if applicable
    if (root && bms_equal(rel->relids, root->all_query_rels))
        pathnode->limit_tuples = root->limit_tuples;
    else
        pathnode->limit_tuples = -1.0;

    // Verify parallel safety of all subpaths
    foreach(l, pathnode->subpaths)
    {
        Path *subpath = (Path *) lfirst(l);
        pathnode->path.parallel_safe = pathnode->path.parallel_safe &&
                                      subpath->parallel_safe;
    }

    // Single-child optimization
    if (list_length(pathnode->subpaths) == 1)
    {
        Path *child = (Path *) linitial(pathnode->subpaths);

        // If parallel awareness matches, inherit child's properties
        if (child->parallel_aware == parallel_aware)
        {
            pathnode->path.rows = child->rows;
            pathnode->path.startup_cost = child->startup_cost;
            pathnode->path.total_cost = child->total_cost;
        }
        else
            cost_append(pathnode);

        pathnode->path.pathkeys = child->pathkeys;
    }
    else
        cost_append(pathnode);

    // Override row estimate if provided
    if (rows >= 0)
        pathnode->path.rows = rows;

    return pathnode;
}
```