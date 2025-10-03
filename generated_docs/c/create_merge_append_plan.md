# create_merge_append_plan

## Location
[src/backend/optimizer/plan/createplan.c:1438-1587](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L1438-L1587)

## Overview
Creates a MergeAppend plan node that merges multiple sorted child plans into a single sorted output stream, commonly used for partitioned table queries where results need to be returned in sorted order.

## Definition

```c
static Plan *
create_merge_append_plan(PlannerInfo *root, MergeAppendPath *best_path,
						 int flags)
```
## Detailed Description
The  function creates a MergeAppend execution plan node from a MergeAppendPath. This plan type is used when the optimizer needs to combine results from multiple child plans (typically from different partitions of a partitioned table) while maintaining a specific sort order. The function ensures that all child plans produce output in the same sort order by potentially adding Sort nodes where necessary.

The function performs several key operations:
1. Creates the MergeAppend node structure and copies generic path information
2. Computes sort column information using  
3. Recursively creates child plans, ensuring they all return compatible target lists
4. Validates that all children have matching sort key information
5. Adds explicit Sort nodes to children that aren't already properly sorted
6. Sets up partition pruning information if enabled and applicable
7. Optionally injects a projection node if the target list was modified during sort preparation

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state and context information
- `*best_path`: MergeAppendPath representing the chosen path with multiple sorted subpaths to merge
- `flags`: Control flags (CP_EXACT_TLIST, CP_SMALL_TLIST, etc.) that affect target list handling
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create MergeAppend node)
  - [build_path_tlist](../b/build_path_tlist.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - [prepare_sort_from_pathkeys](../p/prepare_sort_from_pathkeys.md)
  - [create_plan_recurse](create_plan_recurse.md)
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md)
  - [make_sort](../m/make_sort.md)
  - [label_sort_with_costsize](../l/label_sort_with_costsize.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [make_partition_pruneinfo](../m/make_partition_pruneinfo.md)
  - [inject_projection_plan](../i/inject_projection_plan.md)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md) (main recursive plan creation function)

## Notes and Other Information
- The function assumes all child paths have compatible sort orders that can be merged
- Explicit Sort nodes are only added to children that don't already satisfy the required sort order
- Partition pruning information is collected when  is true and base restriction clauses exist
- The function handles target list adjustments carefully, potentially injecting a projection node if sort columns were added but exact/small target list was requested
- Currently does not support parameterized MergeAppend paths (asserted in the code)
- Used primarily for queries on partitioned tables where sorted results are needed

## Simplified Source

```c
static Plan *
create_merge_append_plan(PlannerInfo *root, MergeAppendPath *best_path, int flags)
{
    MergeAppend *node = makeNode(MergeAppend);
    Plan *plan = &node->plan;
    List *tlist = build_path_tlist(root, &best_path->path);
    int orig_tlist_length = list_length(tlist);
    bool tlist_was_changed;
    List *pathkeys = best_path->path.pathkeys;
    List *subplans = NIL;
    ListCell *subpaths;
    RelOptInfo *rel = best_path->path.parent;
    PartitionPruneInfo *partpruneinfo = NULL;

    // Initialize MergeAppend plan node
    copy_generic_path_info(plan, (Path *) best_path);
    plan->targetlist = tlist;
    plan->qual = NIL;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->apprelids = rel->relids;

    // Compute sort column info and adjust tlist as needed
    (void) prepare_sort_from_pathkeys(plan, pathkeys,
                                      best_path->path.parent->relids,
                                      NULL, true,
                                      &node->numCols,
                                      &node->sortColIdx,
                                      &node->sortOperators,
                                      &node->collations,
                                      &node->nullsFirst);
    tlist_was_changed = (orig_tlist_length != list_length(plan->targetlist));

    // Create child plans with consistent sort order
    foreach(subpaths, best_path->subpaths)
    {
        Path *subpath = (Path *) lfirst(subpaths);
        Plan *subplan;
        int numsortkeys;
        AttrNumber *sortColIdx;
        Oid *sortOperators, *collations;
        bool *nullsFirst;

        // Build child plan with exact tlist requirement
        subplan = create_plan_recurse(root, subpath, CP_EXACT_TLIST);

        // Ensure child matches parent's sort requirements
        subplan = prepare_sort_from_pathkeys(subplan, pathkeys,
                                             subpath->parent->relids,
                                             node->sortColIdx, false,
                                             &numsortkeys, &sortColIdx,
                                             &sortOperators, &collations,
                                             &nullsFirst);

        // Validate sort key consistency
        Assert(numsortkeys == node->numCols);
        if (memcmp(sortColIdx, node->sortColIdx, numsortkeys * sizeof(AttrNumber)) != 0)
            elog(ERROR, "MergeAppend child's targetlist doesn't match MergeAppend");

        // Add Sort node if child isn't sufficiently ordered
        if (!pathkeys_contained_in(pathkeys, subpath->pathkeys))
        {
            Sort *sort = make_sort(subplan, numsortkeys,
                                   sortColIdx, sortOperators,
                                   collations, nullsFirst);
            label_sort_with_costsize(root, sort, best_path->limit_tuples);
            subplan = (Plan *) sort;
        }

        subplans = lappend(subplans, subplan);
    }

    // Setup partition pruning if enabled
    if (enable_partition_pruning)
    {
        List *prunequal = extract_actual_clauses(rel->baserestrictinfo, false);
        Assert(best_path->path.param_info == NULL);

        if (prunequal != NIL)
            partpruneinfo = make_partition_pruneinfo(root, rel,
                                                     best_path->subpaths,
                                                     prunequal);
    }

    node->mergeplans = subplans;
    node->part_prune_info = partpruneinfo;

    // Handle target list projection if needed
    if (tlist_was_changed && (flags & (CP_EXACT_TLIST | CP_SMALL_TLIST)))
    {
        tlist = list_copy_head(plan->targetlist, orig_tlist_length);
        return inject_projection_plan(plan, tlist, plan->parallel_safe);
    }
    else
        return plan;
}
```