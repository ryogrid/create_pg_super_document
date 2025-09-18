# SS_identify_outer_params

## Location
[src/backend/optimizer/plan/subselect.c:2072-2133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L2072-L2133)

## Overview
Identifies and records the set of parameters that will be available from outer query levels to the current query level and its descendants.

## Definition
```c
void SS_identify_outer_params(PlannerInfo *root)
```

## Detailed Description
SS_identify_outer_params computes the complete set of parameter IDs that outer query levels will make available to the current query level and all its descendant levels. This function must be called after both SS_replace_correlation_vars and SS_process_sublinks processing are complete for the current query level and all descendant levels.

The function traverses up the query level hierarchy (via parent_root pointers) and collects parameter IDs from three sources:
1. **Regular parameters** from plan_params lists (Var/PHV/Aggref/GroupingFunc parameters)
2. **InitPlan outputs** from outer-level initialization plans
3. **Worktable parameters** for recursive Common Table Expressions (CTEs)

The collected parameter set is stored in root->outer_params and is later used during final plan cleanup when computing extParam and allParam sets for plan nodes. This deferred approach is necessary because the upper levels plan_params lists are transient and will be destroyed before final plan processing.

## Parameters / Member Variables
- `root`: PlannerInfo structure for the current query level, where the computed outer_params will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [bms_add_member](../b/bms_add_member.md)
  - lfirst_int
- Data types referenced:
  - [Bitmapset](../B/Bitmapset.md)
  - [PlannerInfo](../P/PlannerInfo.md)
  - PlannerParamItem
  - SubPlan
  - ListCell
- Called from (representative examples):
  - [subquery_planner](../s/subquery_planner.md)
  - [build_minmax_path](../b/build_minmax_path.md)

## Notes and Other Information
- Must be called after SS_replace_correlation_vars and SS_process_sublinks are complete
- Early return optimization: if root->glob->paramExecTypes is NIL, no parameters exist in the entire query tree, so no work is needed
- The function walks up the parent_root chain to collect parameters from all outer query levels
- Parameters from initPlans are collected via the setParam list, which contains the parameter IDs that the initPlan will set
- Worktable parameters support recursive CTEs by providing access to the working table
- The computed outer_params bitmap is essential for proper parameter passing in nested query execution
- Part of the subselect processing framework that handles correlated subqueries and parameter passing between query levels