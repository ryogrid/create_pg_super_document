# create_functionscan_plan

## Location
[src/backend/optimizer/plan/createplan.c:3761-3803](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L3761-L3803)

## Overview
Creates a function scan plan for scanning the results of function calls that appear in the FROM clause, treating functions as data sources.

## Definition


## Detailed Description
The  function constructs a FunctionScan execution plan node for scanning the output of function calls that are used as table sources. This handles cases where functions are called in the FROM clause (e.g.,  or ).

The function extracts the function expressions from the range table entry and processes them along with any restriction clauses. It handles both simple function calls and more complex scenarios involving parameter replacement for nested loop joins.

Key processing steps include:
- Extracting function expressions from the range table entry
- Processing and ordering scan restriction clauses
- Replacing outer relation variables with nestloop parameters in both scan clauses and function expressions
- Creating the final FunctionScan plan with ordinality support

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : Path representing the chosen access path for the function scan
- : Target list specifying which columns to return from the function scan
- : List of restriction clauses to apply to the function results

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [make_functionscan](../m/make_functionscan.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)

## Notes and Other Information
- Only works with function relations (RTE_FUNCTION), not tables or subqueries
- Handles both the scan clauses and the function expressions for parameter replacement
- Supports function ordinality (WITH ORDINALITY clause) through rte->funcordinality
- Function expressions themselves can contain nestloop parameters that need replacement
- Used for set-returning functions that can be scanned like tables
- Supports both simple functions and complex lateral function references
- Essential for table functions, generate_series, unnest, and other set-returning functions