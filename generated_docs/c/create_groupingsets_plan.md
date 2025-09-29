# create_groupingsets_plan

## Location
[src/backend/optimizer/plan/createplan.c:2393-2550](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2393-L2550)

## Overview
Creates a plan for GroupingSetsPath operations, implementing SQL GROUPING SETS, ROLLUP, and CUBE functionality by generating a main Agg plan with subsidiary Agg and Sort nodes.

## Definition
```c
static Plan *create_groupingsets_plan(PlannerInfo *root, GroupingSetsPath *best_path)
```

## Detailed Description
The `create_groupingsets_plan` function constructs a complex aggregation plan for handling advanced grouping operations like GROUPING SETS, ROLLUP, and CUBE. It creates a top-level Agg node that implements the last grouping set specified in the GroupingSetsPath, with additional grouping sets represented as subsidiary Agg and Sort nodes in a "chain" list. The function first creates a subplan, builds a grouping map to translate target list references to column positions, and then constructs the chain of subsidiary nodes for intermediate grouping operations. Each rollup in the path gets its own Agg plan with appropriate strategy (hashed, sorted, or plain) and optional Sort node if needed.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state, including processed_groupClause and groupingSets
- `best_path`: GroupingSetsPath structure containing rollups list, aggregation strategy, and cost information

## Dependencies
- Functions called/Symbols referenced:
  - [create_plan_recurse](create_plan_recurse.md)
  - [get_sortgroupclause_tle](../g/get_sortgroupclause_tle.md)
  - [remap_groupColIdx](../r/remap_groupColIdx.md)
  - [make_sort_from_groupcols](../m/make_sort_from_groupcols.md)
  - [make_agg](../m/make_agg.md)
  - [build_path_tlist](../b/build_path_tlist.md)
  - [extract_grouping_ops](../e/extract_grouping_ops.md)
  - [extract_grouping_collations](../e/extract_grouping_collations.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md)

## Notes and Other Information
- The function is static, used only within createplan.c
- Requires that root->parse->groupingSets is not null and rollups is not empty
- Creates and stores a grouping_map in root for later use by setrefs.c to fix GroupingFunc nodes
- Subsidiary nodes in the chain don't participate directly in execution but represent required data for additional steps
- Only the topmost Agg node's costs are meaningful for EXPLAIN output
- Handles three aggregation strategies: AGG_HASHED, AGG_PLAIN, and AGG_SORTED based on rollup characteristics
- Optimizes by removing unnecessary target lists and left trees from subsidiary sort plans to reduce debug output bloat

## Simplified Source

```c
static Plan *
create_groupingsets_plan(PlannerInfo *root, GroupingSetsPath *best_path)
{
    List *rollups = best_path->rollups;

    // Validate inputs
    Assert(root->parse->groupingSets);
    Assert(rollups != NIL);

    // Create subplan with grouping columns available
    Plan *subplan = create_plan_recurse(root, best_path->subpath, CP_LABEL_TLIST);

    // Build grouping map from tleSortGroupRef to column index
    int maxref = 0;
    foreach(lc, root->processed_groupClause)
    {
        SortGroupClause *gc = (SortGroupClause *) lfirst(lc);
        if (gc->tleSortGroupRef > maxref)
            maxref = gc->tleSortGroupRef;
    }

    AttrNumber *grouping_map = (AttrNumber *) palloc0((maxref + 1) * sizeof(AttrNumber));

    // Map sort group references to target list positions
    foreach(lc, root->processed_groupClause)
    {
        SortGroupClause *gc = (SortGroupClause *) lfirst(lc);
        TargetEntry *tle = get_sortgroupclause_tle(gc, subplan->targetlist);
        grouping_map[gc->tleSortGroupRef] = tle->resno;
    }

    // Save grouping map for setrefs.c
    root->grouping_map = grouping_map;

    // Build chain of subsidiary Agg nodes for additional grouping sets
    List *chain = NIL;
    if (list_length(rollups) > 1)
    {
        bool is_first_sort = ((RollupData *) linitial(rollups))->is_hashed;

        for_each_from(lc, rollups, 1)
        {
            RollupData *rollup = lfirst(lc);

            // Remap grouping columns
            AttrNumber *new_grpColIdx = remap_groupColIdx(root, rollup->groupClause);

            // Create Sort node if needed
            Plan *sort_plan = NULL;
            if (!rollup->is_hashed && !is_first_sort)
            {
                sort_plan = (Plan *) make_sort_from_groupcols(rollup->groupClause,
                                                            new_grpColIdx, subplan);
            }

            if (!rollup->is_hashed)
                is_first_sort = false;

            // Determine aggregation strategy
            AggStrategy strat;
            if (rollup->is_hashed)
                strat = AGG_HASHED;
            else if (linitial(rollup->gsets) == NIL)
                strat = AGG_PLAIN;
            else
                strat = AGG_SORTED;

            // Create subsidiary Agg node
            Plan *agg_plan = (Plan *) make_agg(NIL, NIL, strat, AGGSPLIT_SIMPLE,
                                              list_length((List *) linitial(rollup->gsets)),
                                              new_grpColIdx,
                                              extract_grouping_ops(rollup->groupClause),
                                              extract_grouping_collations(rollup->groupClause, subplan->targetlist),
                                              rollup->gsets, NIL,
                                              rollup->numGroups,
                                              best_path->transitionSpace,
                                              sort_plan);

            // Clean up subsidiary nodes to reduce debug output
            if (sort_plan)
            {
                sort_plan->targetlist = NIL;
                sort_plan->lefttree = NULL;
            }

            chain = lappend(chain, agg_plan);
        }
    }

    // Create the main Agg node
    RollupData *rollup = linitial(rollups);
    AttrNumber *top_grpColIdx = remap_groupColIdx(root, rollup->groupClause);
    int numGroupCols = list_length((List *) linitial(rollup->gsets));

    Agg *plan = make_agg(build_path_tlist(root, &best_path->path),
                        best_path->qual,
                        best_path->aggstrategy,
                        AGGSPLIT_SIMPLE,
                        numGroupCols,
                        top_grpColIdx,
                        extract_grouping_ops(rollup->groupClause),
                        extract_grouping_collations(rollup->groupClause, subplan->targetlist),
                        rollup->gsets,
                        chain,
                        rollup->numGroups,
                        best_path->transitionSpace,
                        subplan);

    copy_generic_path_info(&plan->plan, &best_path->path);

    return (Plan *) plan;
}
```