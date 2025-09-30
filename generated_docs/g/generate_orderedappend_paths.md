# generate_orderedappend_paths

## Location
[src/backend/optimizer/path/allpaths.c:1714-1998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L1714-L1998)

## Overview
Generates ordered append paths for append relations, creating either simple Append paths or MergeAppend paths depending on whether child relations naturally provide the required ordering.

## Definition
```c
static void generate_orderedappend_paths(PlannerInfo *root, RelOptInfo *rel, List *live_childrels, List *all_child_pathkeys)
```

## Detailed Description
This function creates ordered paths for append relations by analyzing each distinct ordering (pathkey list) found among child relations. It implements an intelligent optimization: when the required ordering matches the partition order of a partitioned table (either forward or backward), it can generate simple Append paths instead of more expensive MergeAppend paths, since the partitioning scheme guarantees that tuples from earlier partitions come before tuples from later partitions in the sort order.

For each interesting ordering, the function considers three cost scenarios:
1. **Startup-optimized paths** using cheapest startup cost subpaths
2. **Total-cost-optimized paths** using cheapest total cost subpaths  
3. **Fractional paths** optimized for partial result retrieval when `tuple_fraction > 0`

The function handles both RANGE partitioned tables (where partition order matters) and general append relations, automatically choosing between Append and MergeAppend based on whether the partition ordering matches the required sort order.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and optimization context
- `rel`: RelOptInfo structure representing the append relation for which ordered paths are being generated
- `live_childrels`: List of non-dummy child RelOptInfo structures that contribute to the append relation
- `all_child_pathkeys`: List of all distinct pathkey lists (orderings) found among the child relations

## Dependencies
- Functions called/Symbols referenced:
  - IS_SIMPLE_REL (macro to check if relation is a simple base relation)
  - [partitions_are_ordered](../p/partitions_are_ordered.md) (checks if partitions provide natural ordering)
  - [build_partition_pathkeys](../b/build_partition_pathkeys.md) (builds pathkeys for partition column ordering)
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md) (checks if one pathkey list is contained in another)
  - [get_cheapest_path_for_pathkeys](get_cheapest_path_for_pathkeys.md) (finds cheapest path with specific ordering)
  - [get_cheapest_fractional_path_for_pathkeys](get_cheapest_fractional_path_for_pathkeys.md) (finds cheapest fractional path with ordering)
  - [get_singleton_append_subpath](get_singleton_append_subpath.md) (extracts subpath from single-child Append/MergeAppend)
  - [accumulate_append_subpath](../a/accumulate_append_subpath.md) (collects subpaths for MergeAppend construction)
  - [create_append_path](../c/create_append_path.md) (creates simple Append path when partition order matches)
  - [create_merge_append_path](../c/create_merge_append_path.md) (creates MergeAppend path when sorting is required)
- Called from (representative examples):
  - [add_paths_to_append_rel](../a/add_paths_to_append_rel.md) (main path generation for append relations)

## Notes and Other Information
- The function implements a sophisticated optimization for RANGE partitioned tables by detecting when partition order matches required sort order
- When partition ordering matches (forward or backward), simple Append paths are generated instead of expensive MergeAppend paths
- For reverse partition order matching, the function loops backward through child relations to maintain correct ordering
- The function explicitly avoids generating parameterized ordered paths due to their limited utility and high planning cost
- Fractional path support enables efficient partial result retrieval when only a portion of the result set is needed
- The function handles degenerate cases where children don't have paths with required ordering by falling back to unordered cheapest paths

## Simplified Source

```c
static void
generate_orderedappend_paths(PlannerInfo *root, RelOptInfo *rel,
                            List *live_childrels, List *all_child_pathkeys)
{
    List *partition_pathkeys = NIL;
    List *partition_pathkeys_desc = NIL;
    bool partition_pathkeys_partial = true;
    bool partition_pathkeys_desc_partial = true;

    // Check if we can use Append instead of MergeAppend for ordered partitions
    if (rel->part_scheme != NULL && IS_SIMPLE_REL(rel) &&
        partitions_are_ordered(rel->boundinfo, rel->live_parts))
    {
        // Build partition pathkeys for forward and reverse directions
        partition_pathkeys = build_partition_pathkeys(root, rel,
                                                     ForwardScanDirection,
                                                     &partition_pathkeys_partial);
        partition_pathkeys_desc = build_partition_pathkeys(root, rel,
                                                          BackwardScanDirection,
                                                          &partition_pathkeys_desc_partial);
    }

    // Consider each interesting sort ordering
    foreach(lcp, all_child_pathkeys)
    {
        List *pathkeys = (List *) lfirst(lcp);
        List *startup_subpaths = NIL;
        List *total_subpaths = NIL;
        List *fractional_subpaths = NIL;
        bool startup_neq_total = false;
        bool match_partition_order;
        bool match_partition_order_desc;

        // Check if ordering matches partition order (forward or reverse)
        match_partition_order =
            pathkeys_contained_in(pathkeys, partition_pathkeys) ||
            (!partition_pathkeys_partial &&
             pathkeys_contained_in(partition_pathkeys, pathkeys));

        match_partition_order_desc = !match_partition_order &&
            (pathkeys_contained_in(pathkeys, partition_pathkeys_desc) ||
             (!partition_pathkeys_desc_partial &&
              pathkeys_contained_in(partition_pathkeys_desc, pathkeys)));

        // Set loop direction based on partition order match
        int first_index, end_index, direction;
        if (match_partition_order_desc)
        {
            // Loop backward for reverse partition order
            first_index = list_length(live_childrels) - 1;
            end_index = -1;
            direction = -1;
            match_partition_order = true;
        }
        else
        {
            // Loop forward for normal case
            first_index = 0;
            end_index = list_length(live_childrels);
            direction = 1;
        }

        // Collect appropriate child paths
        for (int i = first_index; i != end_index; i += direction)
        {
            RelOptInfo *childrel = list_nth_node(RelOptInfo, live_childrels, i);

            // Find cheapest paths with required ordering
            Path *cheapest_startup = get_cheapest_path_for_pathkeys(
                childrel->pathlist, pathkeys, NULL, STARTUP_COST, false);
            Path *cheapest_total = get_cheapest_path_for_pathkeys(
                childrel->pathlist, pathkeys, NULL, TOTAL_COST, false);

            // Fall back to unordered cheapest if no ordered paths found
            if (cheapest_startup == NULL || cheapest_total == NULL)
            {
                cheapest_startup = cheapest_total = childrel->cheapest_total_path;
            }

            // Handle fractional paths if needed
            Path *cheapest_fractional = NULL;
            if (root->tuple_fraction > 0)
            {
                cheapest_fractional = get_cheapest_fractional_path_for_pathkeys(
                    childrel->pathlist, pathkeys, NULL, 1.0 / root->tuple_fraction);
                if (!cheapest_fractional)
                    cheapest_fractional = cheapest_total;
            }

            if (cheapest_startup != cheapest_total)
                startup_neq_total = true;

            // Accumulate subpaths differently for Append vs MergeAppend
            if (match_partition_order)
            {
                // Use simple Append - extract singleton subpaths
                startup_subpaths = lappend(startup_subpaths,
                                         get_singleton_append_subpath(cheapest_startup));
                total_subpaths = lappend(total_subpaths,
                                       get_singleton_append_subpath(cheapest_total));
                if (cheapest_fractional)
                    fractional_subpaths = lappend(fractional_subpaths,
                                                get_singleton_append_subpath(cheapest_fractional));
            }
            else
            {
                // Use MergeAppend - flatten nested append structures
                accumulate_append_subpath(cheapest_startup, &startup_subpaths, NULL);
                accumulate_append_subpath(cheapest_total, &total_subpaths, NULL);
                if (cheapest_fractional)
                    accumulate_append_subpath(cheapest_fractional, &fractional_subpaths, NULL);
            }
        }

        // Create the appropriate path type
        if (match_partition_order)
        {
            // Create simple Append paths
            add_path(rel, (Path *) create_append_path(root, rel, startup_subpaths,
                                                     NIL, pathkeys, NULL, 0, false, -1));
            if (startup_neq_total)
                add_path(rel, (Path *) create_append_path(root, rel, total_subpaths,
                                                         NIL, pathkeys, NULL, 0, false, -1));
            if (fractional_subpaths)
                add_path(rel, (Path *) create_append_path(root, rel, fractional_subpaths,
                                                         NIL, pathkeys, NULL, 0, false, -1));
        }
        else
        {
            // Create MergeAppend paths
            add_path(rel, (Path *) create_merge_append_path(root, rel,
                                                           startup_subpaths, pathkeys, NULL));
            if (startup_neq_total)
                add_path(rel, (Path *) create_merge_append_path(root, rel,
                                                               total_subpaths, pathkeys, NULL));
            if (fractional_subpaths)
                add_path(rel, (Path *) create_merge_append_path(root, rel,
                                                               fractional_subpaths, pathkeys, NULL));
        }
    }
}
```