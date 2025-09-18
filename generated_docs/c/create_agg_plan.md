# create_agg_plan

## Location
src/backend/optimizer/plan/createplan.c: 2309 - 2354

## Overview
Creates an Agg (aggregation) plan node for the given AggPath, including recursive creation of plans for its subpaths.

## Definition


## Detailed Description
The  function is responsible for creating an Agg plan node from an AggPath structure. This function handles the construction of aggregation plans which are fundamental for implementing SQL GROUP BY operations, aggregate functions (COUNT, SUM, etc.), and HAVING clauses. The function recursively creates the subplan using , builds the target list for the aggregation, processes qualification clauses, and extracts grouping information to construct the final Agg plan node.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : AggPath structure representing the chosen aggregation path with all necessary aggregation details

## Dependencies
- Functions called/Symbols referenced:
  - [create_plan_recurse](create_plan_recurse.md)
  - [build_path_tlist](../b/build_path_tlist.md)
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [make_agg](../m/make_agg.md)
  - [extract_grouping_cols](../e/extract_grouping_cols.md)
  - [extract_grouping_ops](../e/extract_grouping_ops.md)
  - [extract_grouping_collations](../e/extract_grouping_collations.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md)

## Notes and Other Information
- The function is static, indicating it's only used within the createplan.c file
- Agg plans can project, so the function doesn't need to be strict about the child target list, but grouping columns must be available
- The function extracts grouping information including columns, operators, and collations from the AggPath
- Uses CP_LABEL_TLIST flag when creating the subplan to ensure proper target list labeling
- The created plan includes information about aggregation strategy, split mode, number of groups, and transition space requirements