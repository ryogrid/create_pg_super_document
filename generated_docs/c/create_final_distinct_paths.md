# create_final_distinct_paths

## Location
[src/backend/optimizer/plan/planner.c:5099-5305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L5099-L5305)

## Overview
Creates final distinct paths by implementing both sort-based and hash-based DISTINCT operations, optimizing for various scenarios including DISTINCT ON and handling pathkey requirements.

## Definition

```c
static RelOptInfo *
create_final_distinct_paths(PlannerInfo *root, RelOptInfo *input_rel,
							RelOptInfo *distinct_rel)
```
## Detailed Description
This function creates the final execution paths for DISTINCT operations by considering both sort-based and hash-based implementations. It analyzes the input relation's paths and generates optimal distinct paths based on several factors including existing sort order, cost considerations, and configuration settings.

The function handles several key scenarios:
- When input is already grouped/aggregated, it assumes the data is mostly unique
- For ungrouped data, it estimates distinct rows using GROUP BY-comparable analysis
- Implements sort-based DISTINCT using existing sorted paths or creating new sorted paths
- Uses incremental sorting when paths are partially sorted
- Handles the special case where distinct_pathkeys is NIL by applying LIMIT 1
- Provides hash-based DISTINCT as an alternative when sorting is not optimal
- Enforces policy restrictions for DISTINCT ON operations

The function prioritizes sort-based approaches when paths are already sorted, but falls back to hash-based aggregation when no sorted paths are available or when it's more efficient.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing query planning context and distinct/sort pathkeys
- `*input_rel`: RelOptInfo containing source data paths to process for distinct operations
- `*distinct_rel`: RelOptInfo destination relation where created distinct paths will be stored
## Dependencies
- Functions called/Symbols referenced:
  - [get_sortgrouplist_exprs](../g/get_sortgrouplist_exprs.md)
  - [estimate_num_groups](../e/estimate_num_groups.md)
  - [grouping_is_sortable](../g/grouping_is_sortable.md)
  - [pathkeys_count_contained_in](../p/pathkeys_count_contained_in.md)
  - [create_sort_path](create_sort_path.md)
  - [create_incremental_sort_path](create_incremental_sort_path.md)
  - [create_limit_path](create_limit_path.md)
  - [create_upper_unique_path](create_upper_unique_path.md)
  - [grouping_is_hashable](../g/grouping_is_hashable.md)
  - [create_agg_path](create_agg_path.md)
- Called from:
  - [create_distinct_paths](create_distinct_paths.md)
  - [create_partial_distinct_paths](create_partial_distinct_paths.md)

## Notes and Other Information
- For DISTINCT ON queries, uses the more rigorous ordering between DISTINCT and ORDER BY pathkeys
- Handles the special case where distinct_pathkeys becomes NIL by creating LIMIT 1 paths
- [Hash](../H/Hash.md)-based implementation is mandatory when no sort-based paths can be created
- DISTINCT ON operations cannot use hash-based aggregation due to behavioral differences
- Respects enable_hashagg and enable_incremental_sort configuration parameters
- Uses AGGSPLIT_SIMPLE for hash aggregation in the final phase
- Returns the distinct_rel with populated pathlist for subsequent planning phases

## Simplified Source

```c
static RelOptInfo *
create_final_distinct_paths(PlannerInfo *root, RelOptInfo *input_rel,
                           RelOptInfo *distinct_rel)
{
    Query *parse = root->parse;
    Path *cheapest_input_path = input_rel->cheapest_total_path;
    double numDistinctRows;

    // Estimate distinct rows based on whether grouping/aggregation already occurred
    if (parse->groupClause || parse->groupingSets || parse->hasAggs || root->hasHavingQual) {
        // Input already mostly unique from grouping/aggregation
        numDistinctRows = cheapest_input_path->rows;
    } else {
        // Estimate using GROUP BY-comparable analysis
        List *distinctExprs = get_sortgrouplist_exprs(root->processed_distinctClause, parse->targetList);
        numDistinctRows = estimate_num_groups(root, distinctExprs, cheapest_input_path->rows, NULL, NULL);
    }

    // Try sort-based DISTINCT implementations
    if (grouping_is_sortable(root->processed_distinctClause)) {
        List *needed_pathkeys;

        // Choose more rigorous pathkeys for DISTINCT ON
        if (parse->hasDistinctOn &&
            list_length(root->distinct_pathkeys) < list_length(root->sort_pathkeys))
            needed_pathkeys = root->sort_pathkeys;
        else
            needed_pathkeys = root->distinct_pathkeys;

        // Process each input path
        foreach(lc, input_rel->pathlist) {
            Path *input_path = (Path *) lfirst(lc);
            Path *sorted_path;
            bool is_sorted;
            int presorted_keys;

            // Check if path is already sorted appropriately
            is_sorted = pathkeys_count_contained_in(needed_pathkeys, input_path->pathkeys, &presorted_keys);

            if (is_sorted) {
                sorted_path = input_path;
            } else {
                // Skip paths that aren't worth sorting (except cheapest)
                if (input_path != cheapest_input_path &&
                    (presorted_keys == 0 || !enable_incremental_sort))
                    continue;

                // Create sort or incremental sort path
                if (presorted_keys == 0 || !enable_incremental_sort)
                    sorted_path = create_sort_path(root, distinct_rel, input_path, needed_pathkeys, limittuples);
                else
                    sorted_path = create_incremental_sort_path(root, distinct_rel, input_path,
                                                             needed_pathkeys, presorted_keys, limittuples);
            }

            // Handle special case: all pathkeys redundant (use LIMIT 1)
            if (root->distinct_pathkeys == NIL) {
                Node *limitCount = makeConst(INT8OID, -1, InvalidOid, sizeof(int64),
                                           Int64GetDatum(1), false, FLOAT8PASSBYVAL);
                add_path(distinct_rel, create_limit_path(root, distinct_rel, sorted_path,
                                                       NULL, limitCount, LIMIT_OPTION_COUNT, 0, 1));
            } else {
                // Create standard unique path
                add_path(distinct_rel, create_upper_unique_path(root, distinct_rel, sorted_path,
                                                              list_length(root->distinct_pathkeys), numDistinctRows));
            }
        }
    }

    // Try hash-based DISTINCT implementation
    bool allow_hash;
    if (distinct_rel->pathlist == NIL)
        allow_hash = true;  // No alternatives available
    else if (parse->hasDistinctOn || !enable_hashagg)
        allow_hash = false; // Policy restrictions
    else
        allow_hash = true;  // Default case

    if (allow_hash && grouping_is_hashable(root->processed_distinctClause)) {
        // Create hash aggregation path
        add_path(distinct_rel, create_agg_path(root, distinct_rel, cheapest_input_path,
                                             cheapest_input_path->pathtarget, AGG_HASHED,
                                             AGGSPLIT_SIMPLE, root->processed_distinctClause,
                                             NIL, NULL, numDistinctRows));
    }

    return distinct_rel;
}
```