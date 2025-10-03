# generate_union_paths

## Location
[src/backend/optimizer/prep/prepunion.c:696-1017](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepunion.c#L696-L1017)

## Overview
Generates and evaluates multiple execution paths for UNION and UNION ALL operations, creating an optimized RelOptInfo with various path strategies including sorted, hashed, and parallel approaches.

## Definition

```c
static RelOptInfo *
generate_union_paths(SetOperationStmt *op, PlannerInfo *root,
					 List *refnames_tlist,
					 List **pTargetList)
```
## Detailed Description
This function is the core path generation engine for UNION operations in PostgreSQL's query planner. It orchestrates the creation of multiple execution strategies for combining results from union children:

1. **Child Planning**: First calls  to recursively handle nested UNION operations and build the list of child relations
2. **Target List Generation**: Creates the target list for the Append/MergeAppend plan node using 
3. **Path Strategy Selection**: For UNION (not UNION ALL), determines whether sorting or hashing strategies are viable
4. **Multiple Path Creation**: Generates various execution paths:
   - Append path using cheapest paths from each child
   - Parallel Gather+Append path when partial paths are available
   - Hash aggregate path for deduplication (UNION only)
   - Sort+Unique path for deduplication (UNION only)  
   - MergeAppend+Unique path when sorted paths are available (UNION only)

The function handles both UNION and UNION ALL semantics, with UNION requiring deduplication through either hashing or sorting strategies.

## Parameters / Member Variables
- `*op`: SetOperationStmt containing the UNION operation details including the 'all' flag and column types
- `*root`: PlannerInfo containing global planning context and configuration
- `*refnames_tlist`: List of reference names for the target list columns
- `**pTargetList`: Output parameter returning the generated target list for the operation
## Dependencies
- Functions called/Symbols referenced:
  - [plan_union_children](../p/plan_union_children.md)
  - [generate_append_tlist](generate_append_tlist.md)  
  - [generate_setop_grouplist](generate_setop_grouplist.md)
  - [grouping_is_sortable](grouping_is_sortable.md)
  - [grouping_is_hashable](grouping_is_hashable.md)
  - [make_pathkeys_for_sortclauses](../m/make_pathkeys_for_sortclauses.md)
  - [build_setop_child_paths](../b/build_setop_child_paths.md)
  - [create_append_path](../c/create_append_path.md)
  - [create_gather_path](../c/create_gather_path.md)
  - [create_agg_path](../c/create_agg_path.md)
  - [create_sort_path](../c/create_sort_path.md)
  - [create_upper_unique_path](../c/create_upper_unique_path.md)
  - [create_merge_append_path](../c/create_merge_append_path.md)
- Called from (representative examples):
  - [recurse_set_operations](../r/recurse_set_operations.md)

## Notes and Other Information
- For UNION operations, the function assumes worst-case estimates for the number of distinct groups (equal to total input size)
- Parallel execution is considered when all child relations support parallelism and have partial paths
- The function automatically merges identical nested UNION nodes to optimize the plan structure
- When type coercion is required due to mismatching types among union children, sorted paths may become unavailable
- The choice between hash and sort-based deduplication depends on the grouping characteristics of the operation

## Simplified Source

```c
static RelOptInfo *
generate_union_paths(SetOperationStmt *op, PlannerInfo *root,
                     List *refnames_tlist, List **pTargetList)
{
    RelOptInfo *result_rel;
    List *rellist, *tlist_list, *trivial_tlist_list;
    List *cheapest_pathlist = NIL;
    List *partial_pathlist = NIL;
    bool partial_paths_valid = true;
    bool consider_parallel = true;
    List *tlist, *groupList = NIL;
    bool try_sorted = false;
    List *union_pathkeys = NIL;

    // Recursively plan union children, merging identical UNION nodes
    rellist = plan_union_children(root, op, refnames_tlist,
                                 &tlist_list, &trivial_tlist_list);

    // Generate target list for Append/MergeAppend plan node
    tlist = generate_append_tlist(op->colTypes, op->colCollations, false,
                                 tlist_list, refnames_tlist);
    *pTargetList = tlist;

    // For UNION (not UNION ALL), prepare sorting strategy
    if (!op->all) {
        groupList = generate_setop_grouplist(op, tlist);
        if (grouping_is_sortable(op->groupClauses)) {
            try_sorted = true;
            union_pathkeys = make_pathkeys_for_sortclauses(root, groupList, tlist);
            root->query_pathkeys = union_pathkeys;
        }
    }

    // Build child paths and collect cheapest/partial paths
    ListCell *lc, *lc2, *lc3;
    forthree(lc, rellist, lc2, trivial_tlist_list, lc3, tlist_list) {
        RelOptInfo *rel = lfirst(lc);
        bool trivial_tlist = lfirst_int(lc2);
        List *child_tlist = lfirst_node(List, lc3);

        if (rel->rtekind == RTE_SUBQUERY)
            build_setop_child_paths(root, rel, trivial_tlist, child_tlist,
                                   union_pathkeys, NULL);
    }

    // Collect paths from each child relation
    foreach(lc, rellist) {
        RelOptInfo *rel = lfirst(lc);

        cheapest_pathlist = lappend(cheapest_pathlist, rel->cheapest_total_path);

        // Check for sorted paths if trying sorted approach
        if (try_sorted) {
            Path *ordered_path = get_cheapest_path_for_pathkeys(rel->pathlist,
                                                               union_pathkeys, NULL,
                                                               TOTAL_COST, false);
            if (ordered_path == NULL)
                try_sorted = false;  // Give up on sorted approach
        }

        // Handle parallel execution
        if (consider_parallel) {
            if (!rel->consider_parallel || rel->partial_pathlist == NIL)
                partial_paths_valid = false;
            else
                partial_pathlist = lappend(partial_pathlist,
                                         linitial(rel->partial_pathlist));
        }
    }

    // Create result relation and basic append path
    result_rel = fetch_upper_rel(root, UPPERREL_SETOP, bms_union_relids(rellist));
    result_rel->reltarget = create_pathtarget(root, tlist);

    Path *apath = create_append_path(root, result_rel, cheapest_pathlist,
                                   NIL, NIL, NULL, 0, false, -1);
    result_rel->rows = apath->rows;

    // Create parallel gather path if viable
    Path *gpath = NULL;
    if (partial_paths_valid) {
        Path *papath = create_append_path(root, result_rel, NIL, partial_pathlist,
                                        NIL, NULL, parallel_workers,
                                        enable_parallel_append, -1);
        gpath = create_gather_path(root, result_rel, papath,
                                 result_rel->reltarget, NULL, NULL);
    }

    // For UNION (not UNION ALL), add deduplication paths
    if (!op->all) {
        double dNumGroups = apath->rows;  // Conservative estimate
        bool can_hash = grouping_is_hashable(groupList);
        bool can_sort = grouping_is_sortable(groupList);

        // Hash aggregate paths
        if (can_hash) {
            add_path(result_rel, create_agg_path(root, result_rel, apath,
                                               create_pathtarget(root, tlist),
                                               AGG_HASHED, AGGSPLIT_SIMPLE,
                                               groupList, NIL, NULL, dNumGroups));
            if (gpath)
                add_path(result_rel, create_agg_path(root, result_rel, gpath,
                                                   create_pathtarget(root, tlist),
                                                   AGG_HASHED, AGGSPLIT_SIMPLE,
                                                   groupList, NIL, NULL, dNumGroups));
        }

        // Sort + unique paths
        if (can_sort) {
            Path *sorted_path = create_sort_path(root, result_rel, apath,
                                               make_pathkeys_for_sortclauses(root, groupList, tlist), -1.0);
            add_path(result_rel, create_upper_unique_path(root, result_rel, sorted_path,
                                                        list_length(tlist), dNumGroups));
        }
    } else {
        // UNION ALL - just add the append paths
        add_path(result_rel, apath);
        if (gpath)
            add_path(result_rel, gpath);
    }

    return result_rel;
}
```