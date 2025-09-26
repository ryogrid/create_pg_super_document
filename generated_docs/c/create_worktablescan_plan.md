# create_worktablescan_plan

## Location
[src/backend/optimizer/plan/createplan.c:4062-4121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L4062-L4121)

## Overview
Creates a WorkTableScan plan node for scanning the working table of a recursive Common Table Expression (CTE) during recursive query execution.

## Definition
```c
static WorkTableScan *
create_worktablescan_plan(PlannerInfo *root, Path *best_path,
                          List *tlist, List *scan_clauses)
```

## Detailed Description
This function creates a WorkTableScan plan node for accessing the working table used in recursive CTE execution. Recursive CTEs use a working table to store intermediate results during each iteration of the recursive process. The function locates the appropriate worktable parameter ID by traversing up the planner hierarchy to find the plan level that's processing the recursive UNION operation, which is one level below where the CTE is defined. This working table serves as the data source for the recursive arm of the CTE.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `best_path`: Path structure representing the chosen access path for this worktable scan
- `tlist`: Target list specifying which columns/expressions to return from the scan
- `scan_clauses`: List of restriction clauses (WHERE conditions) to apply during scanning

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [make_worktablescan](../m/make_worktablescan.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - [WorkTableScan](../W/WorkTableScan.md) (return type)
  - RTE_CTE (constant)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)

## Notes and Other Information
- The function assumes the scan_relid corresponds to a self-referencing CTE (RTE_CTE with self_reference = true)
- Traverses the planner hierarchy using ctelevelsup to find the recursive UNION processing level, which is one level below the CTE definition
- The worktable parameter ID (wt_param_id) is used during execution to access the working table that stores intermediate results
- Self-referencing CTEs require special handling as they reference themselves in their recursive definition
- Includes comprehensive error checking for missing CTE definitions, invalid level specifications, and missing worktable parameters
- The working table is populated iteratively during recursive CTE execution, with each iteration adding new rows until no more rows are generated