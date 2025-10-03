# get_number_of_groups

## Location
[src/backend/optimizer/plan/planner.c:3698-3819](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L3698-L3819)

## Overview
Estimates the number of groups produced by grouping clauses in a query, returning 1 if not grouping.

## Definition

```c
static double
get_number_of_groups(PlannerInfo *root,
					 double path_rows,
					 grouping_sets_data *gd,
					 List *target_list)
```
## Detailed Description
This function calculates the estimated number of distinct groups that will be produced by GROUP BY clauses in a query. It handles multiple scenarios including plain GROUP BY, GROUPING SETS, empty grouping sets, and aggregation without grouping. For grouping sets, it also annotates the grouping sets data with estimates for each set and rollup list to help determine if some combination could be hashed instead of sorted.

The function processes different cases:
- **GROUPING SETS**: Iterates through rollup data and estimates groups for each grouping set, accumulating totals for both regular and hash-based grouping sets
- **Plain GROUP BY**: Uses the processed group clause to estimate the number of groups
- **Empty grouping sets**: Returns the count of grouping sets (one result row per set)
- **Aggregation without grouping**: Returns 1 (single aggregated result)
- **No grouping**: Returns 1 (pass-through case)

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing query planning information and statistics
- `path_rows`: Number of output rows from the scan/join step, used as input for group estimation
- `*gd`: Grouping sets data structure containing list of grouping sets and their clauses
- `*target_list`: Target list containing group clause references used to extract grouping expressions
## Dependencies
- Functions called/Symbols referenced:
  - [get_sortgrouplist_exprs](get_sortgrouplist_exprs.md)
  - [estimate_num_groups](../e/estimate_num_groups.md)  
  - forboth (macro)
- Data structures used:
  - grouping_sets_data
  - [RollupData](../R/RollupData.md)
  - [GroupingSetData](../G/GroupingSetData.md)
- Called from:
  - standard_qp_extra
  - [create_ordinary_grouping_paths](../c/create_ordinary_grouping_paths.md)
  - [create_partial_grouping_paths](../c/create_partial_grouping_paths.md)

## Notes and Other Information
- This is a static function within the planner module, indicating it's an internal utility for group estimation
- The function is critical for cost-based optimization decisions regarding grouping strategies
- For GROUPING SETS queries, it maintains separate estimates for different grouping approaches (sort-based vs hash-based)
- The estimates are used downstream to choose between different grouping algorithms and determine memory requirements

## Simplified Source

```c
static double
get_number_of_groups(PlannerInfo *root, double path_rows,
                     grouping_sets_data *gd, List *target_list)
{
    Query *parse = root->parse;
    double dNumGroups;

    if (parse->groupClause) {
        List *groupExprs;

        if (parse->groupingSets) {
            // Handle GROUPING SETS: sum estimates for each grouping set
            Assert(gd);
            dNumGroups = 0;

            // Process each rollup in grouping sets
            foreach(lc, gd->rollups) {
                RollupData *rollup = lfirst_node(RollupData, lc);

                groupExprs = get_sortgrouplist_exprs(rollup->groupClause, target_list);
                rollup->numGroups = 0.0;

                // Estimate groups for each grouping set in this rollup
                forboth(lc2, rollup->gsets, lc3, rollup->gsets_data) {
                    List *gset = lfirst(lc2);
                    GroupingSetData *gs = lfirst_node(GroupingSetData, lc3);

                    double numGroups = estimate_num_groups(root, groupExprs, path_rows,
                                                          &gset, NULL);
                    gs->numGroups = numGroups;
                    rollup->numGroups += numGroups;
                }

                dNumGroups += rollup->numGroups;
            }

            // Handle hash-based grouping sets separately
            if (gd->hash_sets_idx) {
                gd->dNumHashGroups = 0;
                groupExprs = get_sortgrouplist_exprs(parse->groupClause, target_list);

                forboth(lc, gd->hash_sets_idx, lc2, gd->unsortable_sets) {
                    List *gset = lfirst(lc);
                    GroupingSetData *gs = lfirst_node(GroupingSetData, lc2);

                    double numGroups = estimate_num_groups(root, groupExprs, path_rows,
                                                          &gset, NULL);
                    gs->numGroups = numGroups;
                    gd->dNumHashGroups += numGroups;
                }

                dNumGroups += gd->dNumHashGroups;
            }
        } else {
            // Plain GROUP BY: estimate based on processed group clause
            groupExprs = get_sortgrouplist_exprs(root->processed_groupClause, target_list);
            dNumGroups = estimate_num_groups(root, groupExprs, path_rows, NULL, NULL);
        }
    } else if (parse->groupingSets) {
        // Empty grouping sets: one result row for each grouping set
        dNumGroups = list_length(parse->groupingSets);
    } else if (parse->hasAggs || root->hasHavingQual) {
        // Plain aggregation: single aggregated result
        dNumGroups = 1;
    } else {
        // No grouping: pass-through
        dNumGroups = 1;
    }

    return dNumGroups;
}
```