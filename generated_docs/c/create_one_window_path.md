# create_one_window_path

## Location
[src/backend/optimizer/plan/planner.c:4660-4829](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L4660-L4829)

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
  - [make_pathkeys_for_window](../m/make_pathkeys_for_window.md)
  - [pathkeys_count_contained_in](../p/pathkeys_count_contained_in.md)
  - [create_sort_path](create_sort_path.md)
  - [create_incremental_sort_path](create_incremental_sort_path.md)
  - [copy_pathtarget](copy_pathtarget.md)
  - [add_column_to_pathtarget](../a/add_column_to_pathtarget.md)
  - [get_typavgwidth](../g/get_typavgwidth.md)
  - [clamp_width_est](clamp_width_est.md)
  - [make_opclause](../m/make_opclause.md)
  - [create_windowagg_path](create_windowagg_path.md)
  - [add_path](../a/add_path.md)
- Called from (representative examples):
  - [create_window_paths](create_window_paths.md)

## Notes and Other Information
- Input target must contain all variables, aggregates, and window partitioning/sorting expressions to ensure they're computed only once at the bottom of the execution stack
- Volatile functions in partitioning/sorting expressions require careful handling to prevent multiple evaluations
- The function supports incremental sorting when enabled and beneficial, reducing sorting overhead for partially ordered input
- [WindowFuncRunCondition](../W/WindowFuncRunCondition.md) elements are converted to OpExpr nodes for runtime evaluation during window function processing
- Each intermediate WindowAgg node includes only the window functions relevant to its window clause
- The topmost WindowAgg node receives the final output target and any accumulated run condition qualifications
- Critical for performance of queries with multiple window functions having different PARTITION BY or ORDER BY clauses

## Simplified Source

```c
static void
create_one_window_path(PlannerInfo *root, RelOptInfo *window_rel, Path *path,
                       PathTarget *input_target, PathTarget *output_target,
                       WindowFuncLists *wflists, List *activeWindows)
{
    PathTarget *window_target = input_target;
    ListCell *l;
    List *topqual = NIL;

    // Stack WindowAgg nodes for each window clause
    foreach(l, activeWindows) {
        WindowClause *wc = lfirst_node(WindowClause, l);
        List *window_pathkeys;
        List *runcondition = NIL;
        int presorted_keys;
        bool is_sorted;
        bool topwindow;

        // Get required pathkeys for this window
        window_pathkeys = make_pathkeys_for_window(root, wc, root->processed_tlist);

        // Check if path is already sorted appropriately
        is_sorted = pathkeys_count_contained_in(window_pathkeys, path->pathkeys,
                                                &presorted_keys);

        // Add sorting if necessary
        if (!is_sorted) {
            if (presorted_keys == 0 || !enable_incremental_sort)
                path = (Path *) create_sort_path(root, window_rel, path,
                                                 window_pathkeys, -1.0);
            else
                path = (Path *) create_incremental_sort_path(root, window_rel, path,
                                                             window_pathkeys,
                                                             presorted_keys, -1.0);
        }

        // Prepare target for this window level
        if (lnext(activeWindows, l)) {
            // Intermediate level: add current window functions to target
            int64 tuple_width = window_target->width;
            window_target = copy_pathtarget(window_target);

            foreach(lc2, wflists->windowFuncs[wc->winref]) {
                WindowFunc *wfunc = lfirst_node(WindowFunc, lc2);
                add_column_to_pathtarget(window_target, (Expr *) wfunc, 0);
                tuple_width += get_typavgwidth(wfunc->wintype, -1);
            }
            window_target->width = clamp_width_est(tuple_width);
        } else {
            // Top level: use final output target
            window_target = output_target;
        }

        topwindow = foreach_current_index(l) == list_length(activeWindows) - 1;

        // Process run conditions for this window
        foreach(lc2, wflists->windowFuncs[wc->winref]) {
            WindowFunc *wfunc = lfirst_node(WindowFunc, lc2);
            foreach(lc3, wfunc->runCondition) {
                WindowFuncRunCondition *wfuncrc = lfirst_node(WindowFuncRunCondition, lc3);

                // Create comparison expression
                Expr *leftop = wfuncrc->wfunc_left ?
                              (Expr *) copyObject(wfunc) : copyObject(wfuncrc->arg);
                Expr *rightop = wfuncrc->wfunc_left ?
                               copyObject(wfuncrc->arg) : (Expr *) copyObject(wfunc);

                Expr *opexpr = make_opclause(wfuncrc->opno, BOOLOID, false,
                                             leftop, rightop, InvalidOid,
                                             wfuncrc->inputcollid);

                runcondition = lappend(runcondition, opexpr);
                if (!topwindow)
                    topqual = lappend(topqual, opexpr);
            }
        }

        // Create WindowAgg path for this level
        path = (Path *) create_windowagg_path(root, window_rel, path, window_target,
                                              wflists->windowFuncs[wc->winref],
                                              runcondition, wc,
                                              topwindow ? topqual : NIL, topwindow);
    }

    add_path(window_rel, path);
}
```