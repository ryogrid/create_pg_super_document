# create_one_window_path

## Location
src/backend/optimizer/plan/planner.c: 4660 - 4829

## Overview
Constructs a complete window function execution path by stacking WindowAgg nodes for each window clause, handling sorting requirements and run conditions between window operations.

## Definition
```c
static void create_one_window_path(PlannerInfo *root,
                                 RelOptInfo *window_rel,
                                 Path *path,
                                 PathTarget *input_target,
                                 PathTarget *output_target,
                                 WindowFuncLists *wflists,
                                 List *activeWindows)
```

## Detailed Description
This function builds a complete execution plan for window function evaluation by creating a stack of WindowAgg nodes, one for each window clause that requires different sorting or partitioning. The function handles the complex orchestration of multiple window operations that may have different ordering requirements.

Key functionalities include:
- **Multi-level window stacking**: Creates separate WindowAgg nodes for each distinct window clause, allowing different partitioning and ordering requirements
- **Sort optimization**: Intelligently chooses between complete sorting and incremental sorting based on existing path ordering and configuration settings
- **Target management**: Manages PathTarget evolution as window functions are added at each level, ensuring proper column propagation
- **Run condition processing**: Handles WindowFuncRunCondition elements by converting them to executable OpExpr comparisons
- **Width estimation**: Accurately tracks tuple width changes as window functions are added to intermediate results

The function processes window clauses in the order determined by select_active_windows, which is assumed to be optimal for execution. Each window clause may require its own sort order, necessitating intermediate sort operations between window evaluation levels.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global query planning context and configuration
- `window_rel`: Target RelOptInfo to receive the completed window execution path
- `path`: Input path that provides source data (must match input_target specification)
- `input_target`: PathTarget defining required input columns including partitioning and sorting expressions
- `output_target`: PathTarget defining the final output columns after all window function evaluation
- `wflists`: WindowFuncLists structure containing organized window functions by window reference
- `activeWindows`: Ordered list of WindowClause structures representing window operations to perform

## Dependencies
- Functions called/Symbols referenced:
  - make_pathkeys_for_window
  - pathkeys_count_contained_in
  - create_sort_path
  - create_incremental_sort_path
  - copy_pathtarget
  - add_column_to_pathtarget
  - get_typavgwidth
  - clamp_width_est
  - make_opclause
  - create_windowagg_path
  - add_path
- Called from (representative examples):
  - create_window_paths

## Notes and Other Information
- Input target must contain all variables, aggregates, and window partitioning/sorting expressions to ensure they're computed only once at the bottom of the execution stack
- Volatile functions in partitioning/sorting expressions require careful handling to prevent multiple evaluations
- The function supports incremental sorting when enabled and beneficial, reducing sorting overhead for partially ordered input
- WindowFuncRunCondition elements are converted to OpExpr nodes for runtime evaluation during window function processing
- Each intermediate WindowAgg node includes only the window functions relevant to its window clause
- The topmost WindowAgg node receives the final output target and any accumulated run condition qualifications
- Critical for performance of queries with multiple window functions having different PARTITION BY or ORDER BY clauses