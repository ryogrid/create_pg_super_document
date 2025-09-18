# create_final_distinct_paths

## Location
src/backend/optimizer/plan/planner.c: 5099 - 5305

## Overview
Creates final distinct paths by implementing both sort-based and hash-based DISTINCT operations, optimizing for various scenarios including DISTINCT ON and handling pathkey requirements.

## Definition


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
- : PlannerInfo structure containing query planning context and distinct/sort pathkeys
- : RelOptInfo containing source data paths to process for distinct operations  
- : RelOptInfo destination relation where created distinct paths will be stored

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
- Hash-based implementation is mandatory when no sort-based paths can be created
- DISTINCT ON operations cannot use hash-based aggregation due to behavioral differences
- Respects enable_hashagg and enable_incremental_sort configuration parameters
- Uses AGGSPLIT_SIMPLE for hash aggregation in the final phase
- Returns the distinct_rel with populated pathlist for subsequent planning phases