# create_gather_merge_plan

## Location
[src/backend/optimizer/plan/createplan.c:1958-2018](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L1958-L2018)

## Overview
Creates a GatherMerge plan node that performs ordered parallel execution by collecting and merging sorted results from multiple worker processes.

## Definition

```c
static GatherMerge *
create_gather_merge_plan(PlannerInfo *root, GatherMergePath *best_path)
```
## Detailed Description
The  function creates a GatherMerge plan node that coordinates parallel execution while preserving the sort order of results. Unlike regular Gather nodes that simply collect results in any order, GatherMerge performs an ordered merge of sorted streams from worker processes.

Key implementation details:
- **Ordered merge**: Merges sorted results from multiple worker processes while maintaining the overall sort order
- **Sort validation**: Verifies that the subplan is already sufficiently sorted for the required pathkeys, as additional sorting cannot be safely added at this level due to potential parallel-unsafe expressions
- **Projection pushdown**: Like Gather, pushes projection work to worker processes using CP_EXACT_TLIST for parallelization
- **Sort metadata**: Uses  to compute sort column information including operators, collations, and null ordering

The function creates the necessary sort infrastructure by populating sortColIdx, sortOperators, collations, and nullsFirst arrays that specify how to perform the ordered merge.

## Parameters / Member Variables
- : PlannerInfo containing planner state and execution context
- : GatherMergePath specifying the parallel merge strategy, pathkeys for ordering, and number of workers

## Dependencies
- Functions called/Symbols referenced:
  - [build_path_tlist](../b/build_path_tlist.md)
  - [create_plan_recurse](create_plan_recurse.md) (with CP_EXACT_TLIST flag)
  - makeNode
  - [copy_generic_path_info](copy_generic_path_info.md)
  - [assign_special_exec_param](../a/assign_special_exec_param.md)
  - [prepare_sort_from_pathkeys](../p/prepare_sort_from_pathkeys.md)
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md)

## Notes and Other Information
- Requires non-empty pathkeys (sort order) - if no ordering is needed, a regular Gather should be used instead
- The subplan must already be sorted according to the required pathkeys; the function cannot add additional sorting due to potential parallel safety issues
- Uses a rescan parameter for coordinating rescans across parallel workers
- Automatically enables parallel mode by setting 
- The merge operation is performed using the sort operators, collations, and null handling specifications computed from the pathkeys
- Essential for queries that need both parallelism and ordered results, such as ORDER BY clauses or merge joins

## Simplified Source

```c
static GatherMerge *
create_gather_merge_plan(PlannerInfo *root, GatherMergePath *best_path)
{
    // Build target list and create subplan
    List *tlist = build_path_tlist(root, &best_path->path);
    Plan *subplan = create_plan_recurse(root, best_path->subpath, CP_EXACT_TLIST);

    // Create GatherMerge plan node
    GatherMerge *gm_plan = makeNode(GatherMerge);
    gm_plan->plan.targetlist = tlist;
    gm_plan->num_workers = best_path->num_workers;

    // Set up parallel coordination parameter
    gm_plan->rescan_param = assign_special_exec_param(root);

    // Prepare sort information for merge operation
    List *pathkeys = best_path->path.pathkeys;
    Assert(pathkeys != NIL); // GatherMerge requires ordering

    subplan = prepare_sort_from_pathkeys(subplan, pathkeys,
                                        best_path->subpath->parent->relids,
                                        gm_plan->sortColIdx, false,
                                        &gm_plan->numCols,
                                        &gm_plan->sortColIdx,
                                        &gm_plan->sortOperators,
                                        &gm_plan->collations,
                                        &gm_plan->nullsFirst);

    // Verify subplan is sufficiently sorted
    if (!pathkeys_contained_in(pathkeys, best_path->subpath->pathkeys))
        elog(ERROR, "gather merge input not sufficiently sorted");

    // Finalize the plan
    gm_plan->plan.lefttree = subplan;
    copy_generic_path_info(&gm_plan->plan, &best_path->path);

    // Enable parallel mode
    root->glob->parallelModeNeeded = true;

    return gm_plan;
}
```