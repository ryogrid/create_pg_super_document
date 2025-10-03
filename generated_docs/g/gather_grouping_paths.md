# gather_grouping_paths

## Location
[src/backend/optimizer/plan/planner.c:7578-7662](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L7578-L7662)

## Overview
Generates optimized Gather and Gather Merge paths for grouping relations by creating both unsorted gather operations and sorted gather merge operations with intelligent sorting strategies.

## Definition

```c
static void
gather_grouping_paths(PlannerInfo *root, RelOptInfo *rel)
```
## Detailed Description
This function is specifically designed for grouped or partially grouped relations and creates parallel execution paths to collect results from worker processes. It performs several key optimizations:

1. **Pathkey trimming**: Removes ORDER BY/DISTINCT aggregate pathkeys that are no longer needed after partial aggregation
2. **Dual gather strategy**: Uses generate_useful_gather_paths for standard gather operations on existing paths
3. **Smart sorting decisions**: For each unsorted partial path, decides between full sorting vs incremental sorting based on presorted keys
4. **Gather Merge optimization**: Creates Gather Merge paths that efficiently combine sorted results from parallel workers
5. **Cost-based selection**: Only considers sorting paths that are likely beneficial (cheapest path or partially presorted paths with incremental sort enabled)

The function ensures optimal parallel result collection by balancing sorting costs against merge benefits, particularly for group-by operations where maintaining order can significantly improve performance.

## Parameters / Member Variables
- `*root`: PlannerInfo containing query planning context, group pathkeys, and other metadata
- `*rel`: RelOptInfo representing the grouped or partially grouped relation for which to generate gather paths
## Dependencies
- Functions called/Symbols referenced:
  - [list_copy_head](../l/list_copy_head.md)
  - [generate_useful_gather_paths](generate_useful_gather_paths.md)
  - [pathkeys_count_contained_in](../p/pathkeys_count_contained_in.md)
  - [create_sort_path](../c/create_sort_path.md)
  - [create_incremental_sort_path](../c/create_incremental_sort_path.md)
  - [create_gather_merge_path](../c/create_gather_merge_path.md)
  - [add_path](../a/add_path.md)
- Called from (representative examples):
  - [add_paths_to_grouping_rel](../a/add_paths_to_grouping_rel.md)
  - [create_ordinary_grouping_paths](../c/create_ordinary_grouping_paths.md)
  - standard_qp_extra

## Notes and Other Information
- Should only be used with grouped or partially grouped relations due to explicit group_pathkeys references
- Passes 'true' as third argument to generate_useful_gather_paths, indicating this is for grouped relations
- Implements intelligent sorting strategy: full sort for unordered paths, incremental sort for partially ordered paths
- Considers enable_incremental_sort setting when deciding between sorting strategies
- Trims pathkeys to exclude ORDER BY/DISTINCT aggregate keys that are handled post-aggregation
- Creates Gather Merge paths with proper total_groups estimation for accurate parallel cost calculation
- Location: src/backend/optimizer/plan/planner.c:7578-7662

## Simplified Source

```c
static void
gather_grouping_paths(PlannerInfo *root, RelOptInfo *rel)
{
    Path *cheapest_partial_path;
    List *groupby_pathkeys;

    // Trim pathkeys to remove ORDER BY/DISTINCT aggregate keys
    if (list_length(root->group_pathkeys) > root->num_groupby_pathkeys)
        groupby_pathkeys = list_copy_head(root->group_pathkeys, root->num_groupby_pathkeys);
    else
        groupby_pathkeys = root->group_pathkeys;

    // Generate standard gather paths for existing partial paths
    generate_useful_gather_paths(root, rel, true);

    cheapest_partial_path = linitial(rel->partial_pathlist);

    // Create Gather Merge paths with intelligent sorting
    foreach(lc, rel->partial_pathlist) {
        Path *path = lfirst(lc);
        bool is_sorted;
        int presorted_keys;
        double total_groups;

        // Check if path is already sorted by group keys
        is_sorted = pathkeys_count_contained_in(groupby_pathkeys, path->pathkeys, &presorted_keys);
        if (is_sorted)
            continue;  // Already sorted, skip

        // Only consider paths worth sorting
        if (path != cheapest_partial_path &&
            (presorted_keys == 0 || !enable_incremental_sort))
            continue;

        total_groups = path->rows * path->parallel_workers;

        // Choose between full sort and incremental sort
        if (presorted_keys == 0 || !enable_incremental_sort) {
            // Full sort needed
            path = (Path *) create_sort_path(root, rel, path, groupby_pathkeys, -1.0);
        } else {
            // Incremental sort possible
            path = (Path *) create_incremental_sort_path(root, rel, path, groupby_pathkeys,
                                                        presorted_keys, -1.0);
        }

        // Create Gather Merge path to collect sorted results from workers
        path = (Path *) create_gather_merge_path(root, rel, path, rel->reltarget,
                                                groupby_pathkeys, NULL, &total_groups);

        add_path(rel, path);
    }
}
```