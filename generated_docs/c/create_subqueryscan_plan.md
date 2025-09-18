# create_subqueryscan_plan

## Location
src/backend/optimizer/plan/createplan.c: 3702 - 3760

## Overview
Creates a subquery scan plan for accessing the results of a subquery as if it were a base relation, handling recursive plan creation for the nested query.

## Definition


## Detailed Description
The  function constructs a SubqueryScan execution plan node that represents scanning the output of a subquery. This is used when a subquery appears in the FROM clause and is treated as a virtual table that needs to be scanned.

The function handles the complex task of recursively creating the plan for the subquery itself, then wrapping it in a SubqueryScan node. It must manage the transition between different planner contexts (the outer query's context and the subquery's context) and handle parameter passing between the outer and inner query levels.

Key processing steps include:
- Recursively creating the plan for the subquery using its own planner context
- Processing scan clauses for the outer level
- Managing nestloop parameters for both lateral references and outer variables
- Creating the final SubqueryScan plan that wraps the subquery plan

## Parameters / Member Variables
- : PlannerInfo structure for the outer query's planner context
- : SubqueryScanPath representing the chosen access path for the subquery
- : Target list specifying which columns to return from the subquery scan
- : List of restriction clauses to apply to the subquery results

## Dependencies
- Functions called/Symbols referenced:
  - [create_plan](create_plan.md)
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [process_subquery_nestloop_params](../p/process_subquery_nestloop_params.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [make_subqueryscan](../m/make_subqueryscan.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)

## Notes and Other Information
- Only works with subquery relations (RTE_SUBQUERY), not base tables or functions
- Recursively calls  (not ) to handle the different planner context
- Manages complex parameter passing between outer and inner query levels
- Processes lateral references and nestloop parameters in a specific order to avoid duplication
- The subquery plan becomes a child of the SubqueryScan node
- Essential for handling correlated subqueries and lateral joins
- Supports parameterized subqueries through careful nestloop parameter management