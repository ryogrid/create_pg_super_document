# create_append_plan

## Location
[src/backend/optimizer/plan/createplan.c:1217-1437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L1217-L1437)

## Overview
Creates an Append plan node that combines results from multiple subpaths, with support for sorting, asynchronous execution, and partition pruning.

## Definition
```c
static Plan *create_append_plan(PlannerInfo *root, AppendPath *best_path, int flags)
```

## Detailed Description
The `create_append_plan` function builds an Append execution plan that concatenates results from multiple child plans. This is commonly used for operations like UNION ALL, partitioned table access, or inheritance hierarchies. The function handles several complex scenarios: it can generate a dummy Result plan when no subpaths exist, create sorted Append plans by inserting Sort nodes where needed, enable asynchronous execution for eligible subplans, and set up partition pruning information for runtime optimization. The function ensures all child plans produce compatible target lists and manages the complexity of coordinating multiple execution paths.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global query planning information
- `best_path`: AppendPath structure representing the selected append strategy with its subpaths
- `flags`: Control flags affecting plan creation (CP_EXACT_TLIST, CP_SMALL_TLIST, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [build_path_tlist](../b/build_path_tlist.md)
  - [make_result](../m/make_result.md)
  - [makeBoolConst](../m/makeBoolConst.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - [prepare_sort_from_pathkeys](../p/prepare_sort_from_pathkeys.md)
  - [create_plan_recurse](create_plan_recurse.md)
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md)
  - [make_sort](../m/make_sort.md)
  - [label_sort_with_costsize](../l/label_sort_with_costsize.md)
  - [mark_async_capable_plan](../m/mark_async_capable_plan.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [make_partition_pruneinfo](../m/make_partition_pruneinfo.md)
  - [inject_projection_plan](../i/inject_projection_plan.md)
  - [Append](../A/Append.md) (type)
  - [AppendPath](../A/AppendPath.md) (type)
  - [PartitionPruneInfo](../P/PartitionPruneInfo.md) (type)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md)

## Notes and Other Information
- Generates a dummy Result plan with constant-FALSE gating when no subpaths exist (empty relation case)
- For ordered Appends, ensures all children produce the same sort key columns and inserts Sort nodes as needed
- Supports asynchronous execution when enable_async_append is true, pathkeys are NIL, and the path is not parallel_safe
- Implements partition pruning by gathering baserestrictinfo and parameter clauses for runtime pruning
- Handles target list compatibility by ensuring all children return the same tlist structure
- May inject a projection plan to remove sort columns added during planning if exact or small tlist is required
- The nasyncplans counter tracks how many subplans can execute asynchronously
- Uses extensive assertion checking to validate sort key consistency across subplans

## Simplified Source

```c
static Plan *
create_append_plan(PlannerInfo *root, AppendPath *best_path, int flags)
{
    // Build target list for the append operation
    List *tlist = build_path_tlist(root, &best_path->path);
    int orig_tlist_length = list_length(tlist);
    bool tlist_was_changed = false;

    // Handle empty subpaths - generate dummy plan
    if (best_path->subpaths == NIL)
    {
        Plan *plan = (Plan *) make_result(tlist,
                                         (Node *) list_make1(makeBoolConst(false, false)),
                                         NULL);
        copy_generic_path_info(plan, (Path *) best_path);
        return plan;
    }

    // Create the Append plan node
    Append *plan = makeNode(Append);
    plan->plan.targetlist = tlist;
    plan->plan.qual = NIL;
    plan->apprelids = best_path->path.parent->relids;

    // Handle sorting requirements
    int nodenumsortkeys = 0;
    AttrNumber *nodeSortColIdx = NULL;
    List *pathkeys = best_path->path.pathkeys;

    if (pathkeys != NIL)
    {
        // Prepare sort information for the Append node
        prepare_sort_from_pathkeys((Plan *) plan, pathkeys,
                                 best_path->path.parent->relids, NULL, true,
                                 &nodenumsortkeys, &nodeSortColIdx, ...);
        tlist_was_changed = (orig_tlist_length != list_length(plan->plan.targetlist));
    }

    // Build subplans
    List *subplans = NIL;
    int nasyncplans = 0;
    bool consider_async = (enable_async_append && pathkeys == NIL &&
                          !best_path->path.parallel_safe);

    foreach(subpaths, best_path->subpaths)
    {
        Path *subpath = (Path *) lfirst(subpaths);

        // Create subplan with exact target list
        Plan *subplan = create_plan_recurse(root, subpath, CP_EXACT_TLIST);

        // Add Sort node if needed for ordered Append
        if (pathkeys != NIL && !pathkeys_contained_in(pathkeys, subpath->pathkeys))
        {
            Sort *sort = make_sort(subplan, ...);
            label_sort_with_costsize(root, sort, best_path->limit_tuples);
            subplan = (Plan *) sort;
        }

        // Check for async execution capability
        if (consider_async && mark_async_capable_plan(subplan, subpath))
            ++nasyncplans;

        subplans = lappend(subplans, subplan);
    }

    // Set up partition pruning if enabled
    PartitionPruneInfo *partpruneinfo = NULL;
    if (enable_partition_pruning)
    {
        List *prunequal = extract_actual_clauses(rel->baserestrictinfo, false);
        // Add parameter clauses if present
        if (best_path->path.param_info)
        {
            List *prmquals = extract_actual_clauses(
                best_path->path.param_info->ppi_clauses, false);
            prunequal = list_concat(prunequal, prmquals);
        }

        if (prunequal != NIL)
            partpruneinfo = make_partition_pruneinfo(root, rel,
                                                   best_path->subpaths, prunequal);
    }

    // Finalize the Append plan
    plan->appendplans = subplans;
    plan->nasyncplans = nasyncplans;
    plan->first_partial_plan = best_path->first_partial_path;
    plan->part_prune_info = partpruneinfo;

    copy_generic_path_info(&plan->plan, (Path *) best_path);

    // Inject projection if tlist was modified and exact/small tlist required
    if (tlist_was_changed && (flags & (CP_EXACT_TLIST | CP_SMALL_TLIST)))
    {
        tlist = list_copy_head(plan->plan.targetlist, orig_tlist_length);
        return inject_projection_plan((Plan *) plan, tlist, plan->plan.parallel_safe);
    }

    return (Plan *) plan;
}
```