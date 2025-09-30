# create_ordinary_grouping_paths

## Location
[src/backend/optimizer/plan/planner.c:4071-4210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L4071-L4210)

## Overview
Creates grouping paths for ordinary (non-degenerate) GROUP BY cases, handling both sorted and hashed aggregation strategies while supporting partial aggregation and partitionwise aggregation optimization techniques.

## Definition

```c
static void
create_ordinary_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
							   RelOptInfo *grouped_rel,
							   const AggClauseCosts *agg_costs,
							   grouping_sets_data *gd,
							   GroupPathExtraData *extra,
							   RelOptInfo **partially_grouped_rel_p)
```
## Detailed Description
This function is responsible for creating execution paths for ordinary GROUP BY operations in PostgreSQL's query planner. It considers both sorted and hashed aggregation strategies simultaneously to ensure at least one viable approach is found, and provides appropriate error messages when neither works.

The function supports several advanced optimization techniques:
- **Partial aggregation**: Creates partially grouped paths that can be further processed in parallel
- **Partitionwise aggregation**: Leverages table partitioning to perform aggregation on individual partitions
- **Parallel processing**: Integrates with PostgreSQL's parallel query execution framework

The function follows a structured approach: first generating any possible partially grouped paths, then applying partitionwise aggregation if applicable, gathering partial paths, estimating group counts, and finally building the complete set of grouping paths.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and query information
- : RelOptInfo representing the input relation to be grouped
- : RelOptInfo representing the target grouped relation to receive new paths
- : Cost estimates for aggregate functions used in the query
- : grouping_sets_data structure containing grouping set information
- : GroupPathExtraData containing additional parameters like target list and flags
- : Output parameter set to the created partially grouped relation or NULL

## Dependencies
- Functions called/Symbols referenced:
  - [group_by_has_partkey](../g/group_by_has_partkey.md)
  - [create_partial_grouping_paths](create_partial_grouping_paths.md)
  - [create_partitionwise_grouping_paths](create_partitionwise_grouping_paths.md)
  - [gather_grouping_paths](../g/gather_grouping_paths.md)
  - [get_number_of_groups](../g/get_number_of_groups.md)
  - [add_paths_to_grouping_rel](../a/add_paths_to_grouping_rel.md)
  - [set_cheapest](../s/set_cheapest.md)
- Called from (representative examples):
  - [create_grouping_paths](create_grouping_paths.md)
  - [create_partitionwise_grouping_paths](create_partitionwise_grouping_paths.md)

## Notes and Other Information
- The function must consider both sorted and hashed aggregation to provide comprehensive error reporting when aggregation fails
- Hashtable size considerations should not prevent using hashing if sorting is impossible
- Supports integration with Foreign Data Wrappers (FDW) through GetForeignUpperPaths callback
- Provides extension points through create_upper_paths_hook for custom upper-level path creation
- Throws a helpful error message when no GROUP BY implementation can be found, specifically noting datatype compatibility issues between hashing and sorting requirements

## Simplified Source

```c
static void
create_ordinary_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
                              RelOptInfo *grouped_rel,
                              const AggClauseCosts *agg_costs,
                              grouping_sets_data *gd,
                              GroupPathExtraData *extra,
                              RelOptInfo **partially_grouped_rel_p)
{
    Path *cheapest_path = input_rel->cheapest_total_path;
    RelOptInfo *partially_grouped_rel = NULL;
    double dNumGroups;
    PartitionwiseAggregateType patype = PARTITIONWISE_AGGREGATE_NONE;

    // Determine if partitionwise aggregation is possible
    if (extra->patype != PARTITIONWISE_AGGREGATE_NONE && IS_PARTITIONED_REL(input_rel)) {
        if (extra->patype == PARTITIONWISE_AGGREGATE_FULL &&
            group_by_has_partkey(input_rel, extra->targetList, root->parse->groupClause))
            patype = PARTITIONWISE_AGGREGATE_FULL;
        else if ((extra->flags & GROUPING_CAN_PARTIAL_AGG) != 0)
            patype = PARTITIONWISE_AGGREGATE_PARTIAL;
        else
            patype = PARTITIONWISE_AGGREGATE_NONE;
    }

    // Create partial grouping paths if supported
    if ((extra->flags & GROUPING_CAN_PARTIAL_AGG) != 0) {
        bool force_rel_creation = (patype == PARTITIONWISE_AGGREGATE_PARTIAL);

        partially_grouped_rel = create_partial_grouping_paths(root, grouped_rel, input_rel,
                                                            gd, extra, force_rel_creation);
    }

    *partially_grouped_rel_p = partially_grouped_rel;

    // Apply partitionwise aggregation if possible
    if (patype != PARTITIONWISE_AGGREGATE_NONE)
        create_partitionwise_grouping_paths(root, input_rel, grouped_rel,
                                          partially_grouped_rel, agg_costs,
                                          gd, patype, extra);

    // Early return for partial aggregation only
    if (extra->patype == PARTITIONWISE_AGGREGATE_PARTIAL) {
        Assert(partially_grouped_rel);
        if (partially_grouped_rel->pathlist)
            set_cheapest(partially_grouped_rel);
        return;
    }

    // Gather any partially grouped partial paths
    if (partially_grouped_rel && partially_grouped_rel->partial_pathlist) {
        gather_grouping_paths(root, partially_grouped_rel);
        set_cheapest(partially_grouped_rel);
    }

    // Estimate number of groups for cost calculations
    dNumGroups = get_number_of_groups(root, cheapest_path->rows, gd, extra->targetList);

    // Build the main grouping paths (both sorted and hashed)
    add_paths_to_grouping_rel(root, input_rel, grouped_rel,
                            partially_grouped_rel, agg_costs, gd,
                            dNumGroups, extra);

    // Error if no implementation found
    if (grouped_rel->pathlist == NIL)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("could not implement GROUP BY"),
                 errdetail("Some of the datatypes only support hashing, while others only support sorting.")));

    // Allow FDW to add foreign paths
    if (grouped_rel->fdwroutine && grouped_rel->fdwroutine->GetForeignUpperPaths)
        grouped_rel->fdwroutine->GetForeignUpperPaths(root, UPPERREL_GROUP_AGG,
                                                     input_rel, grouped_rel, extra);

    // Allow extensions to add custom paths
    if (create_upper_paths_hook)
        (*create_upper_paths_hook)(root, UPPERREL_GROUP_AGG,
                                 input_rel, grouped_rel, extra);
}
```