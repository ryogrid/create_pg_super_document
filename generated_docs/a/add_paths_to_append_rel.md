# add_paths_to_append_rel

## Location
[src/backend/optimizer/path/allpaths.c:1302-1713](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L1302-L1713)

## Overview
Generates paths for append relations by collecting all parameterizations and orderings from child relations and creating appropriate Append and MergeAppend paths.

## Definition
```c
void add_paths_to_append_rel(PlannerInfo *root, RelOptInfo *rel, List *live_childrels)
```

## Detailed Description
This function is the core path generation engine for append relations. It systematically creates multiple types of append paths:

1. **Unparameterized Append paths** using cheapest total paths from each child
2. **Startup-optimized Append paths** using cheapest startup paths when available
3. **Partial Append paths** for parallel execution using partial paths from children
4. **Parallel-aware Append paths** mixing partial and non-partial paths for optimal parallelism
5. **Ordered Append paths** for each distinct ordering found among children
6. **Parameterized Append paths** for each distinct parameterization set found among children

The function intelligently handles parallel execution by determining the optimal number of workers based on the number of child relations and their individual parallel worker requirements. It also handles special cases like single-child append relations that can inherit ordering from their child paths.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and optimization context
- `rel`: RelOptInfo structure representing the append relation to generate paths for
- `live_childrels`: List of non-dummy child RelOptInfo structures that contribute to the append relation

## Dependencies
- Functions called/Symbols referenced:
  - [accumulate_append_subpath](accumulate_append_subpath.md) (accumulates paths from children for append path creation)
  - [get_cheapest_parallel_safe_total_inner](../g/get_cheapest_parallel_safe_total_inner.md) (finds cheapest parallel-safe non-partial path)
  - PATH_REQ_OUTER (macro to extract required outer relations from path)
  - [compare_pathkeys](../c/compare_pathkeys.md) (compares two pathkey lists for equivalence)
  - [create_append_path](../c/create_append_path.md) (creates AppendPath node with specified subpaths)
  - [generate_orderedappend_paths](../g/generate_orderedappend_paths.md) (creates ordered append paths for different orderings)
  - [get_cheapest_parameterized_child_path](../g/get_cheapest_parameterized_child_path.md) (finds cheapest path with specific parameterization)
  - [add_path](add_path.md)/add_partial_path (adds paths to relation's pathlist)
- Called from (representative examples):
  - [set_append_rel_pathlist](../s/set_append_rel_pathlist.md) (main append relation path generation)
  - [generate_partitionwise_join_paths](../g/generate_partitionwise_join_paths.md) (partitionwise join optimization)
  - [create_partitionwise_grouping_paths](../c/create_partitionwise_grouping_paths.md) (partitionwise grouping optimization)

## Notes and Other Information
- The function implements sophisticated parallel execution planning by calculating optimal worker counts using logarithmic scaling based on child count
- It handles both pure partial paths and mixed partial/non-partial paths for parallel append execution
- Special optimization exists for single-child append relations to inherit ordering from child paths
- The function collects all unique parameterizations and orderings from children to avoid redundant path creation
- [Path](../P/Path.md) validation ensures that only feasible combinations of child paths are used in append path construction
- Parallel append is enabled when both the global setting and relation's parallel safety allow it

## Simplified Source
```c
void
add_paths_to_append_rel(PlannerInfo *root, RelOptInfo *rel,
                       List *live_childrels)
{
    List *subpaths = NIL;
    bool subpaths_valid = true;
    List *startup_subpaths = NIL;
    bool startup_subpaths_valid = true;
    List *partial_subpaths = NIL;
    List *pa_partial_subpaths = NIL;
    List *pa_nonpartial_subpaths = NIL;
    bool partial_subpaths_valid = true;
    bool pa_subpaths_valid;
    List *all_child_pathkeys = NIL;
    List *all_child_outers = NIL;
    ListCell *l;
    double partial_rows = -1;

    // Check if parallel append is feasible
    pa_subpaths_valid = enable_parallel_append && rel->consider_parallel;

    // Collect paths and parameterizations from each child
    foreach(l, live_childrels)
    {
        RelOptInfo *childrel = lfirst(l);
        Path *cheapest_partial_path = NULL;

        // Collect unparameterized cheapest total paths
        if (childrel->pathlist != NIL &&
            childrel->cheapest_total_path->param_info == NULL)
            accumulate_append_subpath(childrel->cheapest_total_path,
                                    &subpaths, NULL);
        else
            subpaths_valid = false;

        // Collect startup paths when needed
        if (rel->consider_startup && childrel->cheapest_startup_path != NULL)
            accumulate_append_subpath(childrel->cheapest_startup_path,
                                    &startup_subpaths, NULL);
        else
            startup_subpaths_valid = false;

        // Collect partial paths for parallel execution
        if (childrel->partial_pathlist != NIL)
        {
            cheapest_partial_path = linitial(childrel->partial_pathlist);
            accumulate_append_subpath(cheapest_partial_path,
                                    &partial_subpaths, NULL);
        }
        else
            partial_subpaths_valid = false;

        // Collect paths for parallel append mixing partial/non-partial
        if (pa_subpaths_valid)
        {
            Path *nppath = get_cheapest_parallel_safe_total_inner(childrel->pathlist);

            if (cheapest_partial_path == NULL && nppath == NULL)
                pa_subpaths_valid = false;
            else if (nppath == NULL ||
                     (cheapest_partial_path != NULL &&
                      cheapest_partial_path->total_cost < nppath->total_cost))
            {
                // Use partial path
                accumulate_append_subpath(cheapest_partial_path,
                                        &pa_partial_subpaths,
                                        &pa_nonpartial_subpaths);
            }
            else
            {
                // Use non-partial path
                accumulate_append_subpath(nppath,
                                        &pa_nonpartial_subpaths,
                                        NULL);
            }
        }

        // Collect unique pathkeys and parameterizations
        foreach(lcp, childrel->pathlist)
        {
            Path *childpath = (Path *) lfirst(lcp);

            // Collect unique pathkeys
            if (childpath->pathkeys != NIL)
            {
                // Add to all_child_pathkeys if not already present
                if (!list_member_ptr(all_child_pathkeys, childpath->pathkeys))
                    all_child_pathkeys = lappend(all_child_pathkeys,
                                               childpath->pathkeys);
            }

            // Collect unique parameterizations
            Relids childouter = PATH_REQ_OUTER(childpath);
            if (childouter && !list_member_ptr(all_child_outers, childouter))
                all_child_outers = lappend(all_child_outers, childouter);
        }
    }

    // Create unparameterized append paths
    if (subpaths_valid)
        add_path(rel, (Path *) create_append_path(root, rel, subpaths, NIL,
                                                NIL, NULL, 0, false, -1));

    if (startup_subpaths_valid)
        add_path(rel, (Path *) create_append_path(root, rel, startup_subpaths,
                                                NIL, NIL, NULL, 0, false, -1));

    // Create partial append paths for parallel execution
    if (partial_subpaths_valid && partial_subpaths != NIL)
    {
        int parallel_workers = calculate_parallel_workers(partial_subpaths,
                                                         live_childrels);
        AppendPath *appendpath = create_append_path(root, rel, NIL,
                                                  partial_subpaths, NIL, NULL,
                                                  parallel_workers,
                                                  enable_parallel_append, -1);
        partial_rows = appendpath->path.rows;
        add_partial_path(rel, (Path *) appendpath);
    }

    // Create mixed partial/non-partial parallel append paths
    if (pa_subpaths_valid && pa_nonpartial_subpaths != NIL)
    {
        int parallel_workers = calculate_parallel_workers(pa_partial_subpaths,
                                                         live_childrels);
        AppendPath *appendpath = create_append_path(root, rel,
                                                  pa_nonpartial_subpaths,
                                                  pa_partial_subpaths,
                                                  NIL, NULL, parallel_workers,
                                                  true, partial_rows);
        add_partial_path(rel, (Path *) appendpath);
    }

    // Generate ordered append paths for each unique ordering
    if (subpaths_valid)
        generate_orderedappend_paths(root, rel, live_childrels,
                                   all_child_pathkeys);

    // Create parameterized append paths
    foreach(l, all_child_outers)
    {
        Relids required_outer = (Relids) lfirst(l);
        List *param_subpaths = collect_parameterized_paths(root, live_childrels,
                                                         required_outer);
        if (param_subpaths)
            add_path(rel, (Path *) create_append_path(root, rel, param_subpaths,
                                                    NIL, NIL, required_outer,
                                                    0, false, -1));
    }

    // Handle single child case with ordered partial paths
    if (list_length(live_childrels) == 1)
        add_single_child_ordered_paths(root, rel, linitial(live_childrels),
                                     partial_rows);
}
```