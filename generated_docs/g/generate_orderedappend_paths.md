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