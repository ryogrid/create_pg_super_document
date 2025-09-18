# SS_process_ctes

## Location
[src/backend/optimizer/plan/subselect.c:880-1055](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L880-L1055)

## Overview
Processes a query's WITH list by determining whether to ignore, inline, or convert each Common Table Expression (CTE) to an initplan based on usage patterns and characteristics.

## Definition
```c
void SS_process_ctes(PlannerInfo *root)
```

## Detailed Description
This function is responsible for handling all Common Table Expressions (CTEs) in a query's WITH clause. For each CTE, it makes strategic decisions about execution:

1. **Ignoring**: Unreferenced SELECT CTEs are ignored as they produce no useful output.
2. **Inlining**: CTEs that meet specific criteria are converted to regular sub-SELECT-in-FROM constructs, allowing better optimization integration.
3. **Initplan Conversion**: CTEs that cannot be inlined are converted to initplans with proper parameter management.

The inlining decision considers multiple factors:
- User preferences (CTEMaterializeAlways/Never flags)
- Reference count (single vs. multiple references)  
- Recursiveness
- Side-effects (non-SELECT commands, volatile functions)
- Self-references to recursive CTEs

For non-inlined CTEs, the function creates SubPlan nodes, manages parameter assignments for communication between CTE scans, and integrates the plans into the global subplan infrastructure.

## Parameters
- `root`: PlannerInfo containing query context and CTE information

## Dependencies
- Functions called/Symbols referenced:
  - [contain_dml](../c/contain_dml.md)
  - [contain_outer_selfref](../c/contain_outer_selfref.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [inline_cte](../i/inline_cte.md)
  - copyObject
  - [subquery_planner](../s/subquery_planner.md)
  - fetch_upper_rel
  - [create_plan](../c/create_plan.md)
  - [get_first_col_type](../g/get_first_col_type.md)
  - [assign_special_exec_param](../a/assign_special_exec_param.md)
  - [cost_subplan](../c/cost_subplan.md)
  - makeNode
  - lappend
  - lappend_int
  - list_make1_int
  - [psprintf](../p/psprintf.md)
- Called from (representative examples):
  - [subquery_planner](../s/subquery_planner.md)

## Notes and Other Information
- Fills in root->cte_plan_ids with parallel list to root->parse->cteList containing subplan IDs or -1 for inlined/ignored CTEs
- CTE scans are not considered for parallelism due to potential side-effects
- Parameter management uses special execution parameters for communication between CteScan nodes
- Inlining decisions balance duplicate computation costs against optimization opportunities
- Error checking ensures CTEs don't request parameters from outer query levels inappropriately