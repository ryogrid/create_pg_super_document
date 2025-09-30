# consider_groupingsets_paths

## Location
[src/backend/optimizer/plan/planner.c:4211-4572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L4211-L4572)

## Overview
Evaluates and generates execution paths for GROUPING SETS queries by considering various combinations of hashing and sorting strategies to optimize memory usage and performance.

## Definition
```c
static void consider_groupingsets_paths(PlannerInfo *root,
                                       RelOptInfo *grouped_rel,
                                       Path *path,
                                       bool is_sorted,
                                       bool can_hash,
                                       grouping_sets_data *gd,
                                       const AggClauseCosts *agg_costs,
                                       double dNumGroups)
```

## Detailed Description
This function is the core decision-making engine for GROUPING SETS query optimization. It intelligently selects between different execution strategies (hashing vs. sorting) based on input characteristics, memory constraints, and data properties.

The function operates in two main modes:
1. **Unsorted Input Mode**: When input is not sorted, it attempts to use hash-based aggregation for all grouping sets, with optimizations to detect coincidentally sorted input and reduce memory usage.
2. **Sorted Input Mode**: When input is pre-sorted, it explores both pure sorting and mixed sort/hash approaches, using a sophisticated knapsack algorithm to determine which grouping sets should be hashed vs. sorted to optimize memory usage.

Key optimizations include:
- **Memory-aware planning**: Uses hash memory limits to decide feasibility of hash-based approaches
- **Knapsack optimization**: Applies the discrete knapsack algorithm to select optimal combination of hashed vs. sorted grouping sets
- **Coincidental sorting detection**: Leverages accidentally sorted input even when not explicitly requested
- **Empty grouping set handling**: Special processing for empty grouping sets which cannot be hashed

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning context and query information
- `grouped_rel`: Target RelOptInfo to receive generated grouping paths
- `path`: Input path to be processed for grouping sets
- `is_sorted`: Boolean indicating if input path provides sorted data
- `can_hash`: Boolean indicating if hashing is permitted (may be false due to constraints like ordered aggregates)
- `gd`: grouping_sets_data structure containing preprocessed information about grouping sets
- `agg_costs`: Cost estimates for aggregate functions in the query
- `dNumGroups`: Estimated number of distinct groups expected

## Dependencies
- Functions called/Symbols referenced:
  - [get_hash_memory_limit](../g/get_hash_memory_limit.md)
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md)
  - [estimate_hashagg_tablesize](../e/estimate_hashagg_tablesize.md)
  - [preprocess_groupclause](../p/preprocess_groupclause.md)
  - [remap_to_groupclause_idx](../r/remap_to_groupclause_idx.md)
  - [DiscreteKnapsack](../D/DiscreteKnapsack.md)
  - [create_groupingsets_path](create_groupingsets_path.md)
- Called from (representative examples):
  - [add_paths_to_grouping_rel](../a/add_paths_to_grouping_rel.md)

## Notes and Other Information
- The function can be called multiple times for different input paths and must not modify input data structures
- Uses a sophisticated knapsack algorithm to balance memory usage vs. sorting costs when both hash and sort options are available
- Handles degenerate cases like empty grouping sets and unsortable columns gracefully
- The knapsack algorithm uses a 5% error margin and scales memory values to avoid integer overflow
- Generated paths are directly added to the grouped_rel rather than being returned
- Critical for performance of complex OLAP queries with multiple grouping dimensions

## Simplified Source

```c
static void
consider_groupingsets_paths(PlannerInfo *root, RelOptInfo *grouped_rel,
                           Path *path, bool is_sorted, bool can_hash,
                           grouping_sets_data *gd, const AggClauseCosts *agg_costs,
                           double dNumGroups) {
    Query *parse = root->parse;
    Size hash_mem_limit = get_hash_memory_limit();

    // Unsorted input: try hash-only approach
    if (!is_sorted) {
        List *new_rollups = NIL;
        RollupData *unhashed_rollup = NULL;
        List *sets_data, *empty_sets_data = NIL, *empty_sets = NIL;
        AggStrategy strat = AGG_HASHED;
        double exclude_groups = 0.0;

        Assert(can_hash);

        // Check if input is coincidentally sorted and can reduce memory usage
        if (gd->rollups &&
            pathkeys_contained_in(root->group_pathkeys, path->pathkeys)) {
            unhashed_rollup = lfirst_node(RollupData, list_head(gd->rollups));
            exclude_groups = unhashed_rollup->numGroups;
        }

        // Estimate hash table size
        double hashsize = estimate_hashagg_tablesize(root, path, agg_costs,
                                                    dNumGroups - exclude_groups);

        // Bail if won't fit in memory (unless no other option)
        if (hashsize > hash_mem_limit && gd->rollups)
            return;

        // Break down rollups into individual grouping sets
        sets_data = list_copy(gd->unsortable_sets);

        ListCell *lc;
        foreach(lc, gd->rollups) {
            RollupData *rollup = lfirst_node(RollupData, lc);
            if (rollup == unhashed_rollup) continue; // Skip sorted rollup

            if (!rollup->hashable)
                return; // Need sorted input but can't get it

            sets_data = list_concat(sets_data, rollup->gsets_data);
        }

        // Process each grouping set
        foreach(lc, sets_data) {
            GroupingSetData *gs = lfirst_node(GroupingSetData, lc);
            if (gs->set == NIL) {
                // Empty sets can't be hashed
                empty_sets_data = lappend(empty_sets_data, gs);
                empty_sets = lappend(empty_sets, NIL);
            } else {
                // Create rollup for hashable set
                RollupData *rollup = makeNode(RollupData);
                rollup->groupClause = preprocess_groupclause(root, gs->set);
                rollup->gsets_data = list_make1(gs);
                rollup->gsets = remap_to_groupclause_idx(rollup->groupClause,
                                                        rollup->gsets_data,
                                                        gd->tleref_to_colnum_map);
                rollup->numGroups = gs->numGroups;
                rollup->hashable = true;
                rollup->is_hashed = true;
                new_rollups = lappend(new_rollups, rollup);
            }
        }

        if (new_rollups == NIL) return;

        // Handle unhashed/empty rollups
        if (unhashed_rollup) {
            new_rollups = lappend(new_rollups, unhashed_rollup);
            strat = AGG_MIXED;
        } else if (empty_sets) {
            RollupData *rollup = makeNode(RollupData);
            rollup->groupClause = NIL;
            rollup->gsets_data = empty_sets_data;
            rollup->gsets = empty_sets;
            rollup->numGroups = list_length(empty_sets);
            rollup->hashable = false;
            rollup->is_hashed = false;
            new_rollups = lappend(new_rollups, rollup);
            strat = AGG_MIXED;
        }

        add_path(grouped_rel, (Path *)
                create_groupingsets_path(root, grouped_rel, path,
                                       (List *) parse->havingQual,
                                       strat, new_rollups, agg_costs));
        return;
    }

    // Sorted input: try mixed approaches
    if (gd->rollups == NIL) return;

    // Try mixed sort/hash approach using knapsack algorithm
    if (can_hash && gd->any_hashable) {
        List *rollups = NIL;
        List *hash_sets = list_copy(gd->unsortable_sets);
        double availspace = hash_mem_limit;

        // Account for unsortable sets
        availspace -= estimate_hashagg_tablesize(root, path, agg_costs,
                                                gd->dNumHashGroups);

        // Use knapsack algorithm to select optimal hash/sort mix
        if (availspace > 0 && list_length(gd->rollups) > 1) {
            int num_rollups = list_length(gd->rollups);
            int *k_weights = palloc(num_rollups * sizeof(int));
            double scale = Max(availspace / (20.0 * num_rollups), 1.0);
            int k_capacity = (int) floor(availspace / scale);

            // Calculate weights for knapsack
            int i = 0;
            ListCell *lc;
            for_each_from(lc, gd->rollups, 1) {
                RollupData *rollup = lfirst_node(RollupData, lc);
                if (rollup->hashable) {
                    double sz = estimate_hashagg_tablesize(root, path, agg_costs,
                                                         rollup->numGroups);
                    k_weights[i] = (int) Min(floor(sz / scale), k_capacity + 1.0);
                    i++;
                }
            }

            // Apply knapsack algorithm
            Bitmapset *hash_items = NULL;
            if (i > 0)
                hash_items = DiscreteKnapsack(k_capacity, i, k_weights, NULL);

            // Build rollup lists based on knapsack result
            if (!bms_is_empty(hash_items)) {
                rollups = list_make1(linitial(gd->rollups));

                i = 0;
                for_each_from(lc, gd->rollups, 1) {
                    RollupData *rollup = lfirst_node(RollupData, lc);
                    if (rollup->hashable) {
                        if (bms_is_member(i, hash_items))
                            hash_sets = list_concat(hash_sets, rollup->gsets_data);
                        else
                            rollups = lappend(rollups, rollup);
                        i++;
                    } else {
                        rollups = lappend(rollups, rollup);
                    }
                }
            }
        }

        if (!rollups && hash_sets)
            rollups = list_copy(gd->rollups);

        // Create hashed rollups for selected sets
        foreach(lc, hash_sets) {
            GroupingSetData *gs = lfirst_node(GroupingSetData, lc);
            RollupData *rollup = makeNode(RollupData);

            rollup->groupClause = preprocess_groupclause(root, gs->set);
            rollup->gsets_data = list_make1(gs);
            rollup->gsets = remap_to_groupclause_idx(rollup->groupClause,
                                                    rollup->gsets_data,
                                                    gd->tleref_to_colnum_map);
            rollup->numGroups = gs->numGroups;
            rollup->hashable = true;
            rollup->is_hashed = true;
            rollups = lcons(rollup, rollups);
        }

        if (rollups) {
            add_path(grouped_rel, (Path *)
                    create_groupingsets_path(root, grouped_rel, path,
                                           (List *) parse->havingQual,
                                           AGG_MIXED, rollups, agg_costs));
        }
    }

    // Try pure sorted approach
    if (!gd->unsortable_sets) {
        add_path(grouped_rel, (Path *)
                create_groupingsets_path(root, grouped_rel, path,
                                       (List *) parse->havingQual,
                                       AGG_SORTED, gd->rollups, agg_costs));
    }
}
```