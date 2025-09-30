# create_partitionwise_grouping_paths

## Location
[src/backend/optimizer/plan/planner.c:7940-8083](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L7940-L8083)

## Overview
Creates partitionwise grouping and aggregation paths for partitioned relations by breaking down aggregation into per-partition operations followed by combining results via append operations.

## Definition

```c
structure as is and then override the
		 * members specific to this child.
		 */
		memcpy(&child_extra, extra, sizeof(child_extra));
```
## Detailed Description
This function optimizes aggregation and grouping operations over partitioned relations by implementing partitionwise processing. It handles two main scenarios:

1. **Full partitionwise aggregation**: When all partition keys are included in the GROUP BY clause, each group's rows come from a single partition, allowing complete aggregation per partition followed by simple appending of results.

2. **Partial partitionwise aggregation**: When GROUP BY doesn't contain all partition keys, rows from a group may span multiple partitions. The function performs partial aggregation on each partition, appends the results, and then finalizes the aggregation.

The function iterates through each live partition of the input relation, creates child-specific grouping relations, and generates appropriate grouping paths. It translates expressions and qualifiers for each child partition using append relation information, then creates ordinary grouping paths for each child.

## Parameters / Member Variables
- : PlannerInfo containing global planning context and state information
- : RelOptInfo for the partitioned input relation to be grouped/aggregated
- : RelOptInfo for the final grouped relation that will contain fully aggregated results
- : RelOptInfo for partially grouped results (used in partial partitionwise aggregation)
- : AggClauseCosts structure containing cost estimates for aggregate functions
- : grouping_sets_data containing information about grouping sets operations
- : PartitionwiseAggregateType indicating whether to use full or partial partitionwise aggregation
- : GroupPathExtraData containing additional information like target lists and having qualifiers

## Dependencies
- Functions called/Symbols referenced:
  - [bms_next_member](../b/bms_next_member.md)
  - IS_DUMMY_REL
  - [copy_pathtarget](copy_pathtarget.md)
  - [find_appinfos_by_relids](../f/find_appinfos_by_relids.md)
  - [adjust_appendrel_attrs](../a/adjust_appendrel_attrs.md)
  - [make_grouping_rel](../m/make_grouping_rel.md)
  - [create_ordinary_grouping_paths](create_ordinary_grouping_paths.md)
  - [set_cheapest](../s/set_cheapest.md)
  - [add_paths_to_append_rel](../a/add_paths_to_append_rel.md)
- Called from (representative examples):
  - [create_ordinary_grouping_paths](create_ordinary_grouping_paths.md)

## Notes and Other Information
- The function only processes live (non-dummy) partitions to avoid unnecessary work
- Expression translation is performed for each child partition to account for different column references
- Partial grouping validity is tracked - if any child cannot produce a partially grouped path, partial partitionwise aggregation is disabled
- The function is designed to be no worse than normal aggregation approaches and often performs better, especially when partition elimination can occur or when partial aggregation significantly reduces group counts

## Simplified Source

```c
static void
create_partitionwise_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
                                    RelOptInfo *grouped_rel, RelOptInfo *partially_grouped_rel,
                                    const AggClauseCosts *agg_costs, grouping_sets_data *gd,
                                    PartitionwiseAggregateType patype, GroupPathExtraData *extra)
{
    List *grouped_live_children = NIL;
    List *partially_grouped_live_children = NIL;
    PathTarget *target = grouped_rel->reltarget;
    bool partial_grouping_valid = true;
    int i;

    Assert(patype != PARTITIONWISE_AGGREGATE_NONE);
    Assert(patype != PARTITIONWISE_AGGREGATE_PARTIAL || partially_grouped_rel != NULL);

    // Process each live partition
    i = -1;
    while ((i = bms_next_member(input_rel->live_parts, i)) >= 0) {
        RelOptInfo *child_input_rel = input_rel->part_rels[i];
        Assert(child_input_rel != NULL);

        // Skip dummy partitions
        if (IS_DUMMY_REL(child_input_rel))
            continue;

        // Prepare child-specific data structures
        PathTarget *child_target = copy_pathtarget(target);
        GroupPathExtraData child_extra;
        memcpy(&child_extra, extra, sizeof(child_extra));

        // Get append relation info for expression translation
        AppendRelInfo **appinfos;
        int nappinfos;
        appinfos = find_appinfos_by_relids(root, child_input_rel->relids, &nappinfos);

        // Translate expressions for this child partition
        child_target->exprs = (List *)
            adjust_appendrel_attrs(root, (Node *) target->exprs, nappinfos, appinfos);
        child_extra.havingQual = (Node *)
            adjust_appendrel_attrs(root, extra->havingQual, nappinfos, appinfos);
        child_extra.targetList = (List *)
            adjust_appendrel_attrs(root, (Node *) extra->targetList, nappinfos, appinfos);
        child_extra.patype = patype;

        // Create grouping relation for this child
        RelOptInfo *child_grouped_rel = make_grouping_rel(root, child_input_rel, child_target,
                                                         extra->target_parallel_safe,
                                                         child_extra.havingQual);

        // Generate grouping paths for this child
        RelOptInfo *child_partially_grouped_rel;
        create_ordinary_grouping_paths(root, child_input_rel, child_grouped_rel,
                                      agg_costs, gd, &child_extra,
                                      &child_partially_grouped_rel);

        // Track children for append operations
        if (child_partially_grouped_rel) {
            partially_grouped_live_children = lappend(partially_grouped_live_children,
                                                     child_partially_grouped_rel);
        } else {
            partial_grouping_valid = false;
        }

        if (patype == PARTITIONWISE_AGGREGATE_FULL) {
            set_cheapest(child_grouped_rel);
            grouped_live_children = lappend(grouped_live_children, child_grouped_rel);
        }

        pfree(appinfos);
    }

    // Create append paths for partially grouped children
    if (partially_grouped_rel && partial_grouping_valid) {
        Assert(partially_grouped_live_children != NIL);

        add_paths_to_append_rel(root, partially_grouped_rel, partially_grouped_live_children);

        // Set cheapest path for finalization step
        if (partially_grouped_rel->pathlist)
            set_cheapest(partially_grouped_rel);
    }

    // Create append paths for fully grouped children
    if (patype == PARTITIONWISE_AGGREGATE_FULL) {
        Assert(grouped_live_children != NIL);
        add_paths_to_append_rel(root, grouped_rel, grouped_live_children);
    }
}
```