# fix_placeholder_input_needed_levels

## Location
[src/backend/optimizer/util/placeholder.c:300-328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/placeholder.c#L300-L328)

## Overview
Adjusts the "needed at" levels for placeholder inputs to ensure all variables and placeholders required for placeholder evaluation are available at the appropriate scan or join level.

## Definition
```c
void fix_placeholder_input_needed_levels(PlannerInfo *root)
```

## Detailed Description
This function is called after determining the eval_at levels for all placeholders. It ensures that all variables and placeholders needed to evaluate each placeholder will be available at the scan or join level where the evaluation will be done. The function is particularly important for LATERAL references within placeholder expressions, which need to cause the referenced variables or placeholders to be marked as needed in the scan where they're evaluated.

The function iterates through all placeholders in the planner's placeholder list, extracts all variables from each placeholder's expression (including aggregates, window functions, and nested placeholders), and then adds these variables to the target list at the placeholder's evaluation level.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the placeholder list and other planning context

## Dependencies
- Functions called/Symbols referenced:
  - [PlaceHolderInfo](../P/PlaceHolderInfo.md) (struct type)
  - [pull_var_clause](../p/pull_var_clause.md) (extracts variables from expression nodes)
  - PVC_RECURSE_AGGREGATES (flag for variable extraction)
  - PVC_RECURSE_WINDOWFUNCS (flag for variable extraction)
  - PVC_INCLUDE_PLACEHOLDERS (flag for variable extraction)
  - [add_vars_to_targetlist](../a/add_vars_to_targetlist.md) (adds variables to target list at specified level)
  - [list_free](../l/list_free.md) (memory cleanup)
- Called from (representative examples):
  - [query_planner](../q/query_planner.md) (src/backend/optimizer/plan/planmain.c:218)

## Notes and Other Information
- This function can have side-effects on the ph_needed sets of other PlaceHolderInfos, but this is acceptable because the function doesn't examine ph_needed itself, avoiding ordering issues
- The function handles scan-level evaluations even though they might seem uninteresting, because LATERAL references require proper variable marking
- The loop processes placeholders without ordering concerns due to the design of not examining ph_needed within the function