# create_ordered_paths

## Location
[src/backend/optimizer/plan/planner.c:5306-5520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L5306-L5520)

## Overview
Builds a new upperrel containing paths for ORDER BY evaluation, ensuring all paths satisfy the required ordering through explicit sorting or incremental sorting optimizations.

## Definition

```c
static RelOptInfo *
create_ordered_paths(PlannerInfo *root,
					 RelOptInfo *input_rel,
					 PathTarget *target,
					 bool target_parallel_safe,
					 double limit_tuples)
```
## Detailed Description
This function creates execution paths for ORDER BY operations by building an UPPERREL_ORDERED relation containing paths that satisfy the sort requirements. The function implements intelligent sorting strategies by first checking if input paths are already sorted according to the required sort_pathkeys, and only creating new sorted paths when necessary.

The function handles both serial and parallel execution scenarios:
- For serial paths, it considers full sorts and incremental sorts on existing paths
- For parallel execution, it generates Gather Merge paths by sorting partial paths and combining them
- It applies projection steps when the sorted path's target doesn't match the required target
- It integrates with FDW systems for distributed query processing

The optimization strategy prioritizes reusing existing sort order and applies incremental sorting when paths are partially sorted, which can significantly reduce sorting costs compared to full sorts.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and sort_pathkeys requirements
- : RelOptInfo containing source data paths to be sorted
- : PathTarget specifying the output target list the result paths must emit
- : Boolean indicating whether the target is safe for parallel execution
- : Estimated bound on number of output tuples, or -1 if no LIMIT or couldn't estimate

## Dependencies
- Functions called/Symbols referenced:
  - [fetch_upper_rel](../f/fetch_upper_rel.md)
  - [pathkeys_count_contained_in](../p/pathkeys_count_contained_in.md)
  - [create_sort_path](create_sort_path.md)
  - [create_incremental_sort_path](create_incremental_sort_path.md)
  - [apply_projection_to_path](../a/apply_projection_to_path.md)
  - [create_gather_merge_path](create_gather_merge_path.md)
  - [add_path](../a/add_path.md)
- Called from:
  - [grouping_planner](../g/grouping_planner.md)

## Notes and Other Information
- Only considers sort_pathkeys, unlike generate_useful_gather_paths which looks at other pathkeys
- Preserves FDW relationship information from input to ordered relation
- For parallel execution, creates Gather Merge paths when sorting partial paths makes sense
- Respects enable_incremental_sort configuration parameter for optimization decisions
- Uses limit_tuples parameter to optimize sort operations when LIMIT is present
- The function ensures at least one path is available in the result (Assert at end)
- Does not call set_cheapest as grouping_planner handles that responsibility
- Supports extension hooks for custom path generation via create_upper_paths_hook

## Simplified Source

```c
static RelOptInfo *create_ordered_paths(PlannerInfo *root,
                                       RelOptInfo *input_rel,
                                       PathTarget *target,
                                       bool target_parallel_safe,
                                       double limit_tuples)
{
    Path *cheapest_input_path = input_rel->cheapest_total_path;
    RelOptInfo *ordered_rel;
    ListCell *lc;

    // Create the UPPERREL_ORDERED relation
    ordered_rel = fetch_upper_rel(root, UPPERREL_ORDERED, NULL);

    // Set parallel safety
    if (input_rel->consider_parallel && target_parallel_safe)
        ordered_rel->consider_parallel = true;

    // Preserve FDW information
    ordered_rel->serverid = input_rel->serverid;
    ordered_rel->userid = input_rel->userid;
    ordered_rel->useridiscurrent = input_rel->useridiscurrent;
    ordered_rel->fdwroutine = input_rel->fdwroutine;

    // Process serial paths
    foreach(lc, input_rel->pathlist)
    {
        Path *input_path = (Path *) lfirst(lc);
        Path *sorted_path;
        bool is_sorted;
        int presorted_keys;

        // Check if path is already sorted
        is_sorted = pathkeys_count_contained_in(root->sort_pathkeys,
                                               input_path->pathkeys, &presorted_keys);

        if (is_sorted) {
            sorted_path = input_path;
        } else {
            // Skip if not cheapest and no incremental sort benefit
            if (input_path != cheapest_input_path &&
                (presorted_keys == 0 || !enable_incremental_sort))
                continue;

            // Choose between full sort and incremental sort
            if (presorted_keys == 0 || !enable_incremental_sort)
                sorted_path = (Path *) create_sort_path(root, ordered_rel,
                                                       input_path, root->sort_pathkeys,
                                                       limit_tuples);
            else
                sorted_path = (Path *) create_incremental_sort_path(root, ordered_rel,
                                                                   input_path, root->sort_pathkeys,
                                                                   presorted_keys, limit_tuples);
        }

        // Add projection if needed
        if (sorted_path->pathtarget != target)
            sorted_path = apply_projection_to_path(root, ordered_rel,
                                                  sorted_path, target);

        add_path(ordered_rel, sorted_path);
    }

    // Process parallel paths for Gather Merge
    if (ordered_rel->consider_parallel && root->sort_pathkeys != NIL &&
        input_rel->partial_pathlist != NIL)
    {
        Path *cheapest_partial_path = linitial(input_rel->partial_pathlist);

        foreach(lc, input_rel->partial_pathlist)
        {
            Path *input_path = (Path *) lfirst(lc);
            Path *sorted_path;
            bool is_sorted;
            int presorted_keys;
            double total_groups;

            is_sorted = pathkeys_count_contained_in(root->sort_pathkeys,
                                                   input_path->pathkeys, &presorted_keys);

            if (is_sorted)
                continue;

            // Similar logic as serial paths
            if (input_path != cheapest_partial_path &&
                (presorted_keys == 0 || !enable_incremental_sort))
                continue;

            // Create sorted path and wrap with Gather Merge
            if (presorted_keys == 0 || !enable_incremental_sort)
                sorted_path = (Path *) create_sort_path(root, ordered_rel,
                                                       input_path, root->sort_pathkeys,
                                                       limit_tuples);
            else
                sorted_path = (Path *) create_incremental_sort_path(root, ordered_rel,
                                                                   input_path, root->sort_pathkeys,
                                                                   presorted_keys, limit_tuples);

            total_groups = input_path->rows * input_path->parallel_workers;
            sorted_path = (Path *) create_gather_merge_path(root, ordered_rel,
                                                           sorted_path, sorted_path->pathtarget,
                                                           root->sort_pathkeys, NULL, &total_groups);

            // Add projection if needed
            if (sorted_path->pathtarget != target)
                sorted_path = apply_projection_to_path(root, ordered_rel,
                                                      sorted_path, target);

            add_path(ordered_rel, sorted_path);
        }
    }

    // Allow FDW and extension hooks to add paths
    if (ordered_rel->fdwroutine && ordered_rel->fdwroutine->GetForeignUpperPaths)
        ordered_rel->fdwroutine->GetForeignUpperPaths(root, UPPERREL_ORDERED,
                                                     input_rel, ordered_rel, NULL);

    if (create_upper_paths_hook)
        (*create_upper_paths_hook)(root, UPPERREL_ORDERED,
                                  input_rel, ordered_rel, NULL);

    return ordered_rel;
}
```