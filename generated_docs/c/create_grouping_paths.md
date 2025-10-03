# create_grouping_paths

## Location
[src/backend/optimizer/plan/planner.c:3820-3932](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L3820-L3932)

## Overview
Builds a new upper relation containing paths for grouping and/or aggregation, handling both fully grouped and partially grouped execution strategies.

## Definition

```c
static RelOptInfo *
create_grouping_paths(PlannerInfo *root,
					  RelOptInfo *input_rel,
					  PathTarget *target,
					  bool target_parallel_safe,
					  grouping_sets_data *gd)
```
## Detailed Description
This function creates execution paths for GROUP BY and aggregate operations by building an upper relation that contains different strategies for performing grouping and aggregation. It analyzes the query characteristics to determine which grouping methods are feasible (sort-based, hash-based, partial aggregation) and delegates to specialized path creation functions.

The function handles two main scenarios:
- **Degenerate grouping**: Cases where grouping can be optimized away (e.g., grouping by constants)
- **Ordinary grouping**: Standard GROUP BY operations with various execution strategies

For ordinary grouping, it evaluates multiple factors:
- Whether sort-based grouping is possible based on grouping clause properties
- Whether hash-based grouping is viable (checking for DISTINCT/ORDER BY aggregates)
- Whether partial aggregation can be used for parallel execution
- Whether partitionwise aggregation is enabled and applicable

The function also sets up a GroupPathExtraData structure containing flags and metadata that guide the path creation process.

## Parameters / Member Variables
- `*root`: PlannerInfo containing query planning context and statistics
- `*input_rel`: RelOptInfo containing the source data paths from lower query levels
- `*target`: PathTarget specifying what columns and expressions the result paths should compute
- `target_parallel_safe`: Boolean indicating if the target can be computed safely in parallel workers
- `*gd`: Grouping sets data structure with information about grouping sets and clauses
## Dependencies
- Functions called/Symbols referenced:
  - [get_agg_clause_costs](../g/get_agg_clause_costs.md)
  - [make_grouping_rel](../m/make_grouping_rel.md)
  - [is_degenerate_grouping](../i/is_degenerate_grouping.md)
  - [create_degenerate_grouping_paths](create_degenerate_grouping_paths.md)
  - [grouping_is_sortable](../g/grouping_is_sortable.md)
  - [grouping_is_hashable](../g/grouping_is_hashable.md)
  - [can_partial_agg](can_partial_agg.md)
  - [create_ordinary_grouping_paths](create_ordinary_grouping_paths.md)
  - [set_cheapest](../s/set_cheapest.md)
- Data structures used:
  - [AggClauseCosts](../A/AggClauseCosts.md)
  - [GroupPathExtraData](../G/GroupPathExtraData.md)
  - [PathTarget](../P/PathTarget.md)
  - grouping_sets_data
- Called from:
  - standard_qp_extra
  - [grouping_planner](../g/grouping_planner.md)

## Notes and Other Information
- This function is a key entry point for the grouping/aggregation phase of query planning
- It builds both fully grouped paths and partially grouped paths (which require FinalizeAggregate nodes)
- The function uses a flag-based approach to communicate capabilities to downstream path creation functions
- Partially grouped paths are currently only built as partial paths requiring Gather nodes
- The executor limitations around DISTINCT/ORDER BY aggregates with hashing are enforced here
- Partitionwise aggregation support is conditional on user settings and query characteristics

## Simplified Source

```c
static RelOptInfo *
create_grouping_paths(PlannerInfo *root,
                      RelOptInfo *input_rel,
                      PathTarget *target,
                      bool target_parallel_safe,
                      grouping_sets_data *gd)
{
    Query *parse = root->parse;
    RelOptInfo *grouped_rel;
    RelOptInfo *partially_grouped_rel;
    AggClauseCosts agg_costs;

    // Get aggregate cost estimates
    MemSet(&agg_costs, 0, sizeof(AggClauseCosts));
    get_agg_clause_costs(root, AGGSPLIT_SIMPLE, &agg_costs);

    // Create grouping relation to hold aggregated paths
    grouped_rel = make_grouping_rel(root, input_rel, target,
                                    target_parallel_safe, parse->havingQual);

    // Handle degenerate vs ordinary grouping
    if (is_degenerate_grouping(root))
        create_degenerate_grouping_paths(root, input_rel, grouped_rel);
    else
    {
        int flags = 0;
        GroupPathExtraData extra;

        // Determine if sort-based grouping is possible
        if ((gd && gd->rollups != NIL) ||
            grouping_is_sortable(root->processed_groupClause))
            flags |= GROUPING_CAN_USE_SORT;

        // Determine if hash-based grouping is possible
        if ((parse->groupClause != NIL &&
             root->numOrderedAggs == 0 &&
             (gd ? gd->any_hashable : grouping_is_hashable(root->processed_groupClause))))
            flags |= GROUPING_CAN_USE_HASH;

        // Determine if partial aggregation is possible
        if (can_partial_agg(root))
            flags |= GROUPING_CAN_PARTIAL_AGG;

        // Set up extra data for path creation
        extra.flags = flags;
        extra.target_parallel_safe = target_parallel_safe;
        extra.havingQual = parse->havingQual;
        extra.targetList = parse->targetList;
        extra.partial_costs_set = false;

        // Determine partitionwise aggregation support
        if (enable_partitionwise_aggregate && !parse->groupingSets)
            extra.patype = PARTITIONWISE_AGGREGATE_FULL;
        else
            extra.patype = PARTITIONWISE_AGGREGATE_NONE;

        // Create the actual grouping paths
        create_ordinary_grouping_paths(root, input_rel, grouped_rel,
                                       &agg_costs, gd, &extra,
                                       &partially_grouped_rel);
    }

    set_cheapest(grouped_rel);
    return grouped_rel;
}
```