# create_windowagg_path

## Location
[src/backend/optimizer/util/pathnode.c:3485-3554](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L3485-L3554)

## Overview
Creates a pathnode that represents computation of window functions, where the input must be sorted according to the WindowClause's PARTITION keys plus ORDER BY keys.

## Definition

```c
WindowAggPath *
create_windowagg_path(PlannerInfo *root,
					  RelOptInfo *rel,
					  Path *subpath,
					  PathTarget *target,
					  List *windowFuncs,
					  List *runCondition,
					  WindowClause *winclause,
					  List *qual,
					  bool topwindow)
```
## Detailed Description
This function creates a WindowAggPath node that represents the execution of window functions. Window functions are computed over a set of rows related to the current row within a partition, and they require the input to be properly sorted by partition and order keys. The function preserves the input sort order and can handle both top-level and intermediate WindowAgg operations. For costing purposes, it assumes no redundant partitioning or ordering columns and delegates to cost_windowagg for detailed cost calculation. The path can include run conditions for short-circuiting execution and qualification conditions for top-level windows.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state and configuration
- `*rel`: RelOptInfo representing the parent relation associated with the result
- `*subpath`: Path representing the source of input data (must be properly sorted)
- `*target`: PathTarget structure defining the target list to be computed
- `*windowFuncs`: List of WindowFunc structures representing window functions to compute
- `*runCondition`: List of OpExprs used to short-circuit WindowAgg execution when possible
- `*winclause`: WindowClause structure common to all the WindowFuncs being processed
- `*qual`: List of qualification conditions from lower-level WindowAggPaths (must be NIL unless topwindow is true)
- `topwindow`: Boolean flag indicating if this is the top-level WindowAgg (true) or intermediate (false)
## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [cost_windowagg](cost_windowagg.md)
  - Assert
- Called from (representative examples):
  - [create_one_window_path](create_one_window_path.md) (src/backend/optimizer/plan/planner.c:4809)

## Notes and Other Information
- The input data must be sorted according to the WindowClause's PARTITION keys plus ORDER BY keys
- [WindowAgg](../W/WindowAgg.md) preserves the input sort order in its output
- For now, assumes no parameterization (above any joins) for simplification
- Parallel safety depends on the relation's consider_parallel flag and subpath's parallel safety
- The qual parameter can only be set when topwindow is true, enforced by an assertion
- Cost calculation assumes no redundant partitioning or ordering columns for simplicity
- The function adds target evaluation costs on top of the base window aggregation costs
- Run conditions allow for potential short-circuiting of WindowAgg execution to improve performance

## Simplified Source

```c
WindowAggPath *
create_windowagg_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
                      PathTarget *target, List *windowFuncs, List *runCondition,
                      WindowClause *winclause, List *qual, bool topwindow)
{
    // Create new WindowAggPath node
    WindowAggPath *pathnode = makeNode(WindowAggPath);

    // Validate qual parameter (only allowed for top-level windows)
    Assert(qual == NIL || topwindow);

    // Initialize basic path properties
    pathnode->path.pathtype = T_WindowAgg;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = target;
    pathnode->path.param_info = NULL;  // Assume above joins
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel && subpath->parallel_safe;
    pathnode->path.parallel_workers = subpath->parallel_workers;
    pathnode->path.pathkeys = subpath->pathkeys;  // Preserves input sort

    // Set WindowAgg-specific properties
    pathnode->subpath = subpath;
    pathnode->winclause = winclause;
    pathnode->qual = qual;
    pathnode->runCondition = runCondition;
    pathnode->topwindow = topwindow;

    // Calculate window aggregation costs
    cost_windowagg(&pathnode->path, root, windowFuncs, winclause,
                   subpath->startup_cost, subpath->total_cost, subpath->rows);

    // Add target evaluation costs
    pathnode->path.startup_cost += target->cost.startup;
    pathnode->path.total_cost += target->cost.startup +
                                 target->cost.per_tuple * pathnode->path.rows;

    return pathnode;
}
```