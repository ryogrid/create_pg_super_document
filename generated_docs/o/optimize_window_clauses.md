# optimize_window_clauses

## Location
[src/backend/optimizer/plan/planner.c:5784-5923](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L5784-L5923)

## Overview
Optimizes window clauses by calling each window function's support function to determine if frame options can be adjusted for more optimal execution.

## Definition
```c
static void optimize_window_clauses(PlannerInfo *root, WindowFuncLists *wflists)
```

## Detailed Description
This function performs optimization on window clauses by leveraging window function support functions to potentially modify frame options for better execution performance. It iterates through all window clauses and their associated window functions, calling each function's prosupport function to request optimization recommendations.

The function ensures that all window functions within a single window clause agree on the optimized frame options before applying changes. If optimization creates duplicate window clauses, it merges them by consolidating their window functions and updating references accordingly.

Key optimization steps include:
1. Calling support functions for frame option optimization
2. Validating that all window functions in a clause agree on optimizations
3. Detecting and merging duplicate window clauses after optimization
4. Reassigning window function references when merging clauses

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the parsed query and planner state
- `wflists`: WindowFuncLists structure containing arrays of window functions indexed by window reference

## Dependencies
- Functions called/Symbols referenced:
  - [WindowFuncLists](../W/WindowFuncLists.md), WindowClause, WindowFunc (struct types)
  - [SupportRequestOptimizeWindowClause](../S/SupportRequestOptimizeWindowClause.md) (struct type)
  - [get_func_support](../g/get_func_support.md) (function lookup)
  - OidFunctionCall1 (support function call)
  - foreach_current_index (list iteration macro)
  - [equal](../e/equal.md) (node comparison function)
  - [list_concat](../l/list_concat.md) (list manipulation function)
- Called from (representative examples):
  - standard_qp_extra (src/backend/optimizer/plan/planner.c:211)
  - [grouping_planner](../g/grouping_planner.md) (src/backend/optimizer/plan/planner.c:1502)

## Notes and Other Information
- Currently only allows adjustments to WindowClause frameOptions, but the design allows for future extensions
- Support functions can reject optimization requests by returning NULL
- The function performs comprehensive duplicate detection after frame option changes
- Window function references (winref) are updated when clauses are merged
- Static function scope limits usage to the planner.c module
- Optimization is conservative - if any window function in a clause disagrees with the optimization, no changes are made
- The duplicate detection logic matches the same checks performed in transformWindowFuncCall()

## Simplified Source

```c
static void
optimize_window_clauses(PlannerInfo *root, WindowFuncLists *wflists)
{
    List *windowClause = root->parse->windowClause;

    foreach(lc, windowClause) {
        WindowClause *wc = lfirst_node(WindowClause, lc);
        int optimizedFrameOptions = 0;

        Assert(wc->winref <= wflists->maxWinRef);

        // Skip window clauses with no window functions
        if (wflists->windowFuncs[wc->winref] == NIL)
            continue;

        // Check if all window functions agree on frame optimization
        foreach(lc2, wflists->windowFuncs[wc->winref]) {
            WindowFunc *wfunc = lfirst_node(WindowFunc, lc2);
            Oid prosupport = get_func_support(wfunc->winfnoid);

            // Skip if no support function available
            if (!OidIsValid(prosupport))
                break;

            // Request frame optimization from support function
            SupportRequestOptimizeWindowClause req;
            req.type = T_SupportRequestOptimizeWindowClause;
            req.window_clause = wc;
            req.window_func = wfunc;
            req.frameOptions = wc->frameOptions;

            SupportRequestOptimizeWindowClause *res =
                (SupportRequestOptimizeWindowClause *)
                DatumGetPointer(OidFunctionCall1(prosupport,
                                                 PointerGetDatum(&req)));

            // Skip if support function doesn't handle this request
            if (res == NULL)
                break;

            // Store optimized options from first function
            if (foreach_current_index(lc2) == 0)
                optimizedFrameOptions = res->frameOptions;
            // All functions must agree on optimization
            else if (optimizedFrameOptions != res->frameOptions)
                break;
        }

        // Apply optimization if all functions agreed
        if (lc2 == NULL && wc->frameOptions != optimizedFrameOptions) {
            wc->frameOptions = optimizedFrameOptions;

            // Check for duplicate window clauses after optimization
            if (list_length(windowClause) > 1) {
                foreach(lc3, windowClause) {
                    WindowClause *existing_wc = lfirst_node(WindowClause, lc3);

                    if (existing_wc == wc) continue;

                    // Check if clauses are now duplicates
                    if (equal(wc->partitionClause, existing_wc->partitionClause) &&
                        equal(wc->orderClause, existing_wc->orderClause) &&
                        wc->frameOptions == existing_wc->frameOptions &&
                        equal(wc->startOffset, existing_wc->startOffset) &&
                        equal(wc->endOffset, existing_wc->endOffset)) {

                        // Merge window functions into existing clause
                        foreach(lc4, wflists->windowFuncs[wc->winref]) {
                            WindowFunc *wfunc = lfirst_node(WindowFunc, lc4);
                            wfunc->winref = existing_wc->winref;
                        }

                        // Move function lists
                        wflists->windowFuncs[existing_wc->winref] =
                            list_concat(wflists->windowFuncs[existing_wc->winref],
                                        wflists->windowFuncs[wc->winref]);
                        wflists->windowFuncs[wc->winref] = NIL;
                        break;
                    }
                }
            }
        }
    }
}
```