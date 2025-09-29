# create_window_paths

## Location
[src/backend/optimizer/plan/planner.c:4573-4659](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L4573-L4659)

## Overview
Creates a new upper relation containing execution paths for window function evaluation, optimizing path selection based on existing sort orders and parallel safety considerations.

## Definition
```c
static RelOptInfo *create_window_paths(PlannerInfo *root,
                                     RelOptInfo *input_rel,
                                     PathTarget *input_target,
                                     PathTarget *output_target,
                                     bool output_target_parallel_safe,
                                     WindowFuncLists *wflists,
                                     List *activeWindows)
```

## Detailed Description
This function constructs the window relation (UPPERREL_WINDOW) that handles window function computation in PostgreSQL's query planning. It creates execution paths by analyzing existing input paths and determining which ones are suitable for window function evaluation.

The function employs several optimization strategies:
- **Sort optimization**: Prioritizes paths that already satisfy or partially satisfy the required window ordering (root->window_pathkeys) to minimize sorting overhead
- **Parallel safety analysis**: Determines if window operations can be executed in parallel by checking input relation safety, output target safety, and active window constructs
- **FDW integration**: Supports Foreign Data Wrapper extensions for distributed window function computation
- **Path selection**: Considers both the cheapest total path and any paths with beneficial ordering characteristics

The function always includes the cheapest total path (which may require additional sorting) but also considers paths that have some pre-existing order that matches window requirements, even if they're not the cheapest overall.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global query planning context
- `input_rel`: Source RelOptInfo containing input data paths
- `input_target`: PathTarget representing the expected input columns for window operations
- `output_target`: PathTarget representing the final output after window function evaluation
- `output_target_parallel_safe`: Boolean indicating if the output target can be safely computed in parallel
- `wflists`: WindowFuncLists structure containing organized window function information
- `activeWindows`: List of active window specifications for this planning level

## Dependencies
- Functions called/Symbols referenced:
  - [fetch_upper_rel](../f/fetch_upper_rel.md)
  - [is_parallel_safe](../i/is_parallel_safe.md)
  - [pathkeys_count_contained_in](../p/pathkeys_count_contained_in.md)
  - [create_one_window_path](create_one_window_path.md)
  - [set_cheapest](../s/set_cheapest.md)
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md)

## Notes and Other Information
- All input paths are expected to return data matching the input_target specification
- The function creates paths for the (UPPERREL_WINDOW, NULL) upper relation level
- Parallel execution is enabled only when input relation supports it, output target is parallel-safe, and active windows contain no parallel-unsafe constructs
- FDW and extension hook integration allows custom window function implementations
- The function ensures that at least the cheapest path is considered, providing a fallback when no optimally ordered paths exist
- Window function evaluation often requires specific ordering, making sort optimization a critical performance factor

## Simplified Source

```c
static RelOptInfo *create_window_paths(PlannerInfo *root,
                                      RelOptInfo *input_rel,
                                      PathTarget *input_target,
                                      PathTarget *output_target,
                                      bool output_target_parallel_safe,
                                      WindowFuncLists *wflists,
                                      List *activeWindows)
{
    RelOptInfo *window_rel;
    ListCell *lc;

    // Create the UPPERREL_WINDOW relation
    window_rel = fetch_upper_rel(root, UPPERREL_WINDOW, NULL);

    // Set parallel safety
    if (input_rel->consider_parallel && output_target_parallel_safe &&
        is_parallel_safe(root, (Node *) activeWindows))
        window_rel->consider_parallel = true;

    // Preserve FDW information
    window_rel->serverid = input_rel->serverid;
    window_rel->userid = input_rel->userid;
    window_rel->useridiscurrent = input_rel->useridiscurrent;
    window_rel->fdwroutine = input_rel->fdwroutine;

    // Consider paths with beneficial ordering for window functions
    foreach(lc, input_rel->pathlist)
    {
        Path *path = (Path *) lfirst(lc);
        int presorted_keys;

        // Include cheapest path or paths with some window ordering
        if (path == input_rel->cheapest_total_path ||
            pathkeys_count_contained_in(root->window_pathkeys, path->pathkeys,
                                       &presorted_keys) ||
            presorted_keys > 0)
            create_one_window_path(root,
                                  window_rel,
                                  path,
                                  input_target,
                                  output_target,
                                  wflists,
                                  activeWindows);
    }

    // Allow FDW to add paths
    if (window_rel->fdwroutine &&
        window_rel->fdwroutine->GetForeignUpperPaths)
        window_rel->fdwroutine->GetForeignUpperPaths(root, UPPERREL_WINDOW,
                                                    input_rel, window_rel,
                                                    NULL);

    // Allow extensions to add paths
    if (create_upper_paths_hook)
        (*create_upper_paths_hook)(root, UPPERREL_WINDOW,
                                  input_rel, window_rel, NULL);

    // Choose the best paths
    set_cheapest(window_rel);

    return window_rel;
}
```