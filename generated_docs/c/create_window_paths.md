# create_window_paths

## Location
src/backend/optimizer/plan/planner.c: 4573 - 4659

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
  - fetch_upper_rel
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