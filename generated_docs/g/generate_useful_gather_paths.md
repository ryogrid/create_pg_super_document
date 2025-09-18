# generate_useful_gather_paths

## Location
[src/backend/optimizer/path/allpaths.c:3190-3305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L3190-L3305)

## Overview
Generates optimized parallel access paths by creating Gather and GatherMerge paths with intelligent sorting strategies, including both full and incremental sort options for useful orderings.

## Definition
```c
void
generate_useful_gather_paths(PlannerInfo *root, RelOptInfo *rel, bool override_rows)
```

## Detailed Description
This function extends the basic gather path generation by creating more sophisticated parallel execution paths that consider useful orderings for both current and upstream operations. Unlike plain generate_gather_paths, it analyzes pathkeys that might be useful for nodes above the Gather Merge and adds appropriate sorting (regular or incremental) to provide those orderings.

The function operates in multiple phases:
1. First calls generate_gather_paths to create basic gather paths
2. Identifies useful pathkeys using get_useful_pathkeys_for_relation with parallel-safe requirements
3. For each useful ordering, examines all partial paths to determine optimal sorting strategies
4. Creates GatherMerge paths with either full sort or incremental sort based on existing ordering

The function intelligently chooses between full and incremental sort based on the number of presorted keys and configuration settings, prioritizing incremental sort when partial ordering exists.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global query planning information and configuration
- `rel`: RelOptInfo structure representing the relation for which optimized parallel paths are being generated
- `override_rows`: Boolean flag indicating whether to override the relation's row count estimate for specialized operations

## Dependencies
- Functions called/Symbols referenced:
  - [generate_gather_paths](generate_gather_paths.md)
  - [get_useful_pathkeys_for_relation](get_useful_pathkeys_for_relation.md)
  - [pathkeys_count_contained_in](../p/pathkeys_count_contained_in.md)
  - [create_sort_path](../c/create_sort_path.md)
  - [create_incremental_sort_path](../c/create_incremental_sort_path.md)
  - [create_gather_merge_path](../c/create_gather_merge_path.md)
  - [add_path](../a/add_path.md)
  - GatherMergePath (type)
- Called from (representative examples):
  - [set_rel_pathlist](../s/set_rel_pathlist.md)
  - [standard_join_search](../s/standard_join_search.md)
  - [create_partial_distinct_paths](../c/create_partial_distinct_paths.md)
  - [gather_grouping_paths](gather_grouping_paths.md)
  - [apply_scanjoin_target_to_paths](../a/apply_scanjoin_target_to_paths.md)

## Notes and Other Information
- Requires parallel-safe pathkeys for pushing sorts below Gather Merge nodes
- Avoids redundant work by skipping already fully-sorted paths (handled by generate_gather_paths)
- Implements intelligent sort selection: incremental sort for partially-sorted paths, full sort otherwise
- Always processes the cheapest partial path regardless of presorted keys
- Respects enable_incremental_sort configuration setting
- Critical for optimizing complex queries with parallel execution and ordering requirements
- Generates paths that can satisfy both local ordering needs and upstream operation requirements