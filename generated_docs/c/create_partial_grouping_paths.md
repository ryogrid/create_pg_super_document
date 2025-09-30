# create_partial_grouping_paths

## Location
[src/backend/optimizer/plan/planner.c:7279-7577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L7279-L7577)

## Overview
Creates a new upper relation for partial aggregation results and populates it with appropriate paths that perform initial phases of aggregation, preparing data for subsequent finalization steps in parallel query processing.

## Definition

```c
static RelOptInfo *
create_partial_grouping_paths(PlannerInfo *root,
							  RelOptInfo *grouped_rel,
							  RelOptInfo *input_rel,
							  grouping_sets_data *gd,
							  GroupPathExtraData *extra,
							  bool force_rel_creation)
```
## Detailed Description
This function is a key component of PostgreSQL's parallel aggregation strategy. It creates an intermediate relation (UPPERREL_PARTIAL_GROUP_AGG) that represents partially aggregated results requiring subsequent finalization. The function handles:

1. **Partial aggregation path creation**: Generates paths using AGGSPLIT_INITIAL_SERIAL for both sorted and hashed approaches
2. **Parallel processing support**: Creates both partial and non-partial paths depending on parallelism capabilities
3. **Target list optimization**: Builds specialized target lists including necessary Vars and Aggrefs for HAVING clauses
4. **Cost estimation**: Calculates separate costs for partial and final aggregation phases
5. **FDW integration**: Allows foreign data wrappers to contribute custom partial grouping paths

The function intelligently determines whether partial aggregation is beneficial based on available input paths, parallelism settings, and partitionwise aggregation configuration.

## Parameters / Member Variables
- : PlannerInfo containing query planning context and metadata
- : RelOptInfo representing the final grouped relation that will receive finalized results
- : RelOptInfo representing the input relation to be partially aggregated
- : grouping_sets_data containing grouping sets configuration information
- : GroupPathExtraData containing flags, costs, and additional planning parameters
- : Boolean flag to force creation of the relation even when optimization suggests it's unnecessary

## Dependencies
- Functions called/Symbols referenced:
  - [fetch_upper_rel](../f/fetch_upper_rel.md)
  - [make_partial_grouping_target](../m/make_partial_grouping_target.md)
  - [get_agg_clause_costs](../g/get_agg_clause_costs.md)
  - [get_number_of_groups](../g/get_number_of_groups.md)
  - [get_useful_group_keys_orderings](../g/get_useful_group_keys_orderings.md)
  - [make_ordered_path](../m/make_ordered_path.md)
  - [create_agg_path](create_agg_path.md)
  - [create_group_path](create_group_path.md)
  - [add_path](../a/add_path.md)
  - [add_partial_path](../a/add_partial_path.md)
- Called from (representative examples):
  - [create_ordinary_grouping_paths](create_ordinary_grouping_paths.md)
  - standard_qp_extra

## Notes and Other Information
- Returns NULL if no real benefit is found in creating partial aggregation paths (unless force_rel_creation is true)
- Sets up both partial_costs and final_costs for accurate cost estimation of two-phase aggregation
- Supports both partitionwise aggregation (PARTITIONWISE_AGGREGATE_PARTIAL) and regular parallel processing
- The created relation requires subsequent gather_grouping_paths and set_cheapest calls to finalize path lists
- All generated paths use AGGSPLIT_INITIAL_SERIAL aggregation mode, expecting later AGGSPLIT_FINAL_DESERIAL finalization
- Location: src/backend/optimizer/plan/planner.c:7279-7577

## Simplified Source

```c
static RelOptInfo *
create_partial_grouping_paths(PlannerInfo *root, RelOptInfo *grouped_rel,
                              RelOptInfo *input_rel, grouping_sets_data *gd,
                              GroupPathExtraData *extra, bool force_rel_creation)
{
    Query *parse = root->parse;
    AggClauseCosts *agg_partial_costs = &extra->agg_partial_costs;
    AggClauseCosts *agg_final_costs = &extra->agg_final_costs;
    Path *cheapest_partial_path = NULL;
    Path *cheapest_total_path = NULL;
    bool can_hash = (extra->flags & GROUPING_CAN_USE_HASH) != 0;
    bool can_sort = (extra->flags & GROUPING_CAN_USE_SORT) != 0;

    // Determine available input paths for partial aggregation
    if (input_rel->pathlist != NIL &&
        extra->patype == PARTITIONWISE_AGGREGATE_PARTIAL)
        cheapest_total_path = input_rel->cheapest_total_path;

    if (grouped_rel->consider_parallel && input_rel->partial_pathlist != NIL)
        cheapest_partial_path = linitial(input_rel->partial_pathlist);

    // Exit early if no suitable paths and not forced
    if (cheapest_total_path == NULL && cheapest_partial_path == NULL && !force_rel_creation)
        return NULL;

    // Create new relation for partial aggregation results
    RelOptInfo *partially_grouped_rel = fetch_upper_rel(root, UPPERREL_PARTIAL_GROUP_AGG,
                                                        grouped_rel->relids);

    // Copy basic properties from grouped relation
    partially_grouped_rel->consider_parallel = grouped_rel->consider_parallel;
    partially_grouped_rel->reloptkind = grouped_rel->reloptkind;
    partially_grouped_rel->serverid = grouped_rel->serverid;
    partially_grouped_rel->userid = grouped_rel->userid;
    partially_grouped_rel->useridiscurrent = grouped_rel->useridiscurrent;
    partially_grouped_rel->fdwroutine = grouped_rel->fdwroutine;

    // Build specialized target list for partial aggregation
    partially_grouped_rel->reltarget = make_partial_grouping_target(root, grouped_rel->reltarget,
                                                                   extra->havingQual);

    // Set up cost structures for partial and final aggregation phases
    if (!extra->partial_costs_set) {
        MemSet(agg_partial_costs, 0, sizeof(AggClauseCosts));
        MemSet(agg_final_costs, 0, sizeof(AggClauseCosts));
        if (parse->hasAggs) {
            get_agg_clause_costs(root, AGGSPLIT_INITIAL_SERIAL, agg_partial_costs);
            get_agg_clause_costs(root, AGGSPLIT_FINAL_DESERIAL, agg_final_costs);
        }
        extra->partial_costs_set = true;
    }

    // Estimate number of partial groups
    double dNumPartialGroups = 0;
    double dNumPartialPartialGroups = 0;
    if (cheapest_total_path != NULL)
        dNumPartialGroups = get_number_of_groups(root, cheapest_total_path->rows, gd, extra->targetList);
    if (cheapest_partial_path != NULL)
        dNumPartialPartialGroups = get_number_of_groups(root, cheapest_partial_path->rows, gd, extra->targetList);

    // Create sort-based partial aggregation paths from non-partial inputs
    if (can_sort && cheapest_total_path != NULL) {
        foreach(lc, input_rel->pathlist) {
            Path *path = lfirst(lc);
            List *pathkey_orderings = get_useful_group_keys_orderings(root, path);

            foreach(lc2, pathkey_orderings) {
                GroupByOrdering *info = lfirst(lc2);
                path = make_ordered_path(root, partially_grouped_rel, path,
                                       cheapest_total_path, info->pathkeys);
                if (path == NULL) continue;

                if (parse->hasAggs) {
                    add_path(partially_grouped_rel, (Path *)
                            create_agg_path(root, partially_grouped_rel, path,
                                          partially_grouped_rel->reltarget,
                                          parse->groupClause ? AGG_SORTED : AGG_PLAIN,
                                          AGGSPLIT_INITIAL_SERIAL, info->clauses, NIL,
                                          agg_partial_costs, dNumPartialGroups));
                } else {
                    add_path(partially_grouped_rel, (Path *)
                            create_group_path(root, partially_grouped_rel, path,
                                            info->clauses, NIL, dNumPartialGroups));
                }
            }
        }
    }

    // Create sort-based partial aggregation paths from partial inputs
    if (can_sort && cheapest_partial_path != NULL) {
        foreach(lc, input_rel->partial_pathlist) {
            Path *path = lfirst(lc);
            List *pathkey_orderings = get_useful_group_keys_orderings(root, path);

            foreach(lc2, pathkey_orderings) {
                GroupByOrdering *info = lfirst(lc2);
                path = make_ordered_path(root, partially_grouped_rel, path,
                                       cheapest_partial_path, info->pathkeys);
                if (path == NULL) continue;

                if (parse->hasAggs) {
                    add_partial_path(partially_grouped_rel, (Path *)
                                    create_agg_path(root, partially_grouped_rel, path,
                                                  partially_grouped_rel->reltarget,
                                                  parse->groupClause ? AGG_SORTED : AGG_PLAIN,
                                                  AGGSPLIT_INITIAL_SERIAL, info->clauses, NIL,
                                                  agg_partial_costs, dNumPartialPartialGroups));
                } else {
                    add_partial_path(partially_grouped_rel, (Path *)
                                    create_group_path(root, partially_grouped_rel, path,
                                                    info->clauses, NIL, dNumPartialPartialGroups));
                }
            }
        }
    }

    // Add hash-based partial aggregation paths
    if (can_hash && cheapest_total_path != NULL) {
        add_path(partially_grouped_rel, (Path *)
                create_agg_path(root, partially_grouped_rel, cheapest_total_path,
                              partially_grouped_rel->reltarget, AGG_HASHED,
                              AGGSPLIT_INITIAL_SERIAL, root->processed_groupClause,
                              NIL, agg_partial_costs, dNumPartialGroups));
    }

    if (can_hash && cheapest_partial_path != NULL) {
        add_partial_path(partially_grouped_rel, (Path *)
                        create_agg_path(root, partially_grouped_rel, cheapest_partial_path,
                                      partially_grouped_rel->reltarget, AGG_HASHED,
                                      AGGSPLIT_INITIAL_SERIAL, root->processed_groupClause,
                                      NIL, agg_partial_costs, dNumPartialPartialGroups));
    }

    // Allow FDW to add custom partial grouping paths
    if (partially_grouped_rel->fdwroutine &&
        partially_grouped_rel->fdwroutine->GetForeignUpperPaths) {
        FdwRoutine *fdwroutine = partially_grouped_rel->fdwroutine;
        fdwroutine->GetForeignUpperPaths(root, UPPERREL_PARTIAL_GROUP_AGG,
                                        input_rel, partially_grouped_rel, extra);
    }

    return partially_grouped_rel;
}
```