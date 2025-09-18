# create_resultscan_plan

## Location
src/backend/optimizer/plan/createplan.c: 4025 - 4061

## Overview
Creates a Result plan node for scanning an RTE_RESULT base relation that represents a constant result or expression evaluation.

## Definition
```c
static Result *
create_resultscan_plan(PlannerInfo *root, Path *best_path,
                       List *tlist, List *scan_clauses)
```

## Detailed Description
This function creates a Result plan node for handling RTE_RESULT relations, which are used when a query needs to produce a constant result set or evaluate expressions that don't involve scanning actual table data. Examples include SELECT statements without FROM clauses (like SELECT 1, 2, 3) or queries that produce computed results. The function processes any restriction clauses, handles nestloop parameter substitution, and creates a simple Result node that can execute the target expressions and apply any qualifying conditions.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `best_path`: Path structure representing the chosen access path for this result scan
- `tlist`: Target list specifying which columns/expressions to return from the scan
- `scan_clauses`: List of restriction clauses (WHERE conditions) to apply during execution

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - order_qual_clauses
  - extract_actual_clauses
  - replace_nestloop_params
  - make_result
  - copy_generic_path_info
  - Result (return type)
  - RTE_RESULT (constant)
  - PG_USED_FOR_ASSERTS_ONLY (macro)
- Called from (representative examples):
  - create_scan_plan

## Notes and Other Information
- The function assumes the scan_relid corresponds to a result relation (RTE_RESULT)
- RTE_RESULT relations are used for queries that don't scan actual tables but instead compute constant results or expressions
- Common use cases include SELECT statements without FROM clauses, VALUES clauses with constant expressions, or computed subqueries
- The range table entry variable is marked with PG_USED_FOR_ASSERTS_ONLY since it's only used for assertion checking in debug builds
- Unlike other scan types, Result nodes don't require child plans as they generate their output directly from the target list expressions