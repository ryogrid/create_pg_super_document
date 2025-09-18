# set_deparse_context_plan

## Location
src/backend/utils/adt/ruleutils.c: 3776 - 3798

## Overview
Configures a deparse context to focus on a specific Plan node, enabling resolution of OUTER_VAR, INNER_VAR, and INDEX_VAR references in expressions.

## Definition
```c
List *set_deparse_context_plan(List *dpcontext, Plan *plan, List *ancestors)
```

## Detailed Description
This function specializes a deparse context created by deparse_context_for_plan_tree() to work with a specific Plan node. It enables the resolution of special variable types (OUTER_VAR, INNER_VAR, INDEX_VAR) by setting up the plan hierarchy information. OUTER_VAR and INNER_VAR references are resolved by drilling down into the left and right child plans, while INDEX_VAR references are resolved using indextlist from IndexOnlyScan nodes or scan tlist from ForeignScan and CustomScan nodes. The ancestors list enables PARAM_EXEC parameter resolution.

## Parameters / Member Variables
- `dpcontext`: A deparse context list (should contain exactly one deparse_namespace entry for plan deparsing)
- `plan`: The specific Plan node that contains the expressions to be deparsed
- `ancestors`: A list of parent Plan and SubPlan nodes, with the most closely nested first, used for PARAM_EXEC parameter resolution

## Dependencies
- Functions called/Symbols referenced:
  - list_length (for assertion checking)
  - linitial (to get first namespace entry)
  - [set_deparse_plan](set_deparse_plan.md) (to configure the plan-specific information)
- Called from (representative examples):
  - [show_plan_tlist](show_plan_tlist.md) (src/backend/commands/explain.c:2464)
  - [show_expression](show_expression.md) (src/backend/commands/explain.c:2495)
  - [show_grouping_sets](show_grouping_sets.md) (src/backend/commands/explain.c:2638)
  - [show_sort_group_keys](show_sort_group_keys.md) (src/backend/commands/explain.c:2778)
  - [show_memoize_info](show_memoize_info.md) (src/backend/commands/explain.c:3346)

## Notes and Other Information
This function must be called each time you want to deparse expressions from a different Plan node within the same Plan tree. It assumes all Plan nodes in the tree share the same range table, which is set up once by deparse_context_for_plan_tree(). The function does not currently support deparsing indexquals in regular IndexScan or BitmapIndexScan nodes - only the indexqualorig fields can be deparsed for those node types since they don't contain INDEX_VAR references. The function returns the same List that was passed in as a notational convenience.