# create_ctescan_plan

## Location
[src/backend/optimizer/plan/createplan.c:3891-3985](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L3891-L3985)

## Overview
Creates a CteScan plan node for scanning a Common Table Expression (CTE) base relation with the specified target list and scan clauses.

## Definition
```c
static CteScan *
create_ctescan_plan(PlannerInfo *root, Path *best_path,
                    List *tlist, List *scan_clauses)
```

## Detailed Description
This function creates a CteScan plan node for executing a CTE scan operation. It locates the referenced CTE by traversing up the planner hierarchy according to the CTE's nesting level, finds the corresponding SubPlan that was previously created for the CTE, and extracts the CTE parameter ID needed for execution. The function handles restriction clauses optimization and nestloop parameter substitution, ensuring proper integration with the query execution plan.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `best_path`: Path structure representing the chosen access path for this CTE scan
- `tlist`: Target list specifying which columns/expressions to return from the scan
- `scan_clauses`: List of restriction clauses (WHERE conditions) to apply during scanning

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [make_ctescan](../m/make_ctescan.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - [list_nth_int](../l/list_nth_int.md)
  - linitial_int
  - CteScan (return type)
  - SubPlan
  - CommonTableExpr
  - RTE_CTE (constant)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)

## Notes and Other Information
- The function assumes the scan_relid corresponds to a CTE base relation (RTE_CTE) that is not self-referencing
- Traverses the planner hierarchy using ctelevelsup to find the appropriate CTE definition and its associated SubPlan
- Locates the CTE by name matching within the cteList and retrieves the corresponding plan_id from cte_plan_ids
- Extracts the CTE parameter ID from the SubPlan's setParam list, which is used during execution to access the CTE's materialized results
- Includes comprehensive error checking for missing CTE definitions, plans, and parameter configurations