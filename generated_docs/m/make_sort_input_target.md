# make_sort_input_target

## Location
src/backend/optimizer/plan/planner.c: 6328 - 6498

## Overview
Generates the appropriate PathTarget for initial input to Sort step, determining which expressions should be evaluated before or after sorting for optimal performance.

## Definition
```c
static PathTarget *
make_sort_input_target(PlannerInfo *root,
                       PathTarget *final_target,
                       bool *have_postponed_srfs)
```

## Detailed Description
This function chooses the target to be computed by the node just below the Sort (and DISTINCT, if any) steps when the query has ORDER BY. It implements a sophisticated strategy for deciding whether to evaluate expressions before or after sorting, balancing several competing considerations:

**Postponement Policy:**
- **Volatile expressions**: Always postponed to ensure consistent evaluation order
- **Set-returning functions (SRFs)**: Postponed if none appear in sort columns (to avoid bloating sort dataset and maintain output order)
- **Expensive expressions**: Postponed if there's a LIMIT, partial evaluation is possible, or other expressions are already being postponed

**Constraints:**
- All SRFs in the tlist must be evaluated at the same plan step for synchronized execution in nodeProjectSet
- Grouping/ordering columns cannot be postponed as they're needed for sorting
- Aggref and WindowFunc nodes are preserved since they were computed earlier

The function analyzes each column to determine if it contains SRFs, volatile functions, or expensive operations (>10X cpu_operator_cost), then constructs an appropriate input target.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning information
- `final_target`: The query's final target list in PathTarget form
- `have_postponed_srfs`: Output parameter set to true if any SRFs are postponed to after the Sort

## Dependencies
- Functions called/Symbols referenced:
  - get_pathtarget_sortgroupref
  - [expression_returns_set](../e/expression_returns_set.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [cost_qual_eval_node](../c/cost_qual_eval_node.md)
  - [create_empty_pathtarget](../c/create_empty_pathtarget.md)
  - [add_column_to_pathtarget](../a/add_column_to_pathtarget.md)
  - [pull_var_clause](../p/pull_var_clause.md)
  - [add_new_columns_to_pathtarget](../a/add_new_columns_to_pathtarget.md)
  - [set_pathtarget_cost_width](../s/set_pathtarget_cost_width.md)
  - [list_free](../l/list_free.md)
- Called from:
  - [grouping_planner](../g/grouping_planner.md) (src/backend/optimizer/plan/planner.c:1575)
  - standard_qp_extra (src/backend/optimizer/plan/planner.c:219)

## Notes and Other Information
- This is a static function within planner.c
- Assumes parse->sortClause exists (query has ORDER BY)
- Uses 10X cpu_operator_cost as threshold for "expensive" expressions
- The have_postponed_srfs output affects whether Sort can rely on LIMIT to bound rows
- If no postponement is beneficial, returns final_target unchanged
- Uses PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_WINDOWFUNCS, and PVC_INCLUDE_PLACEHOLDERS flags
- Comments note some redundant cost calculation occurs
- The function handles the case where an explicit Sort might not be used in the final plan