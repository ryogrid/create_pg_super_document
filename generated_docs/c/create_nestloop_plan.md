# create_nestloop_plan

## Location
[src/backend/optimizer/plan/createplan.c:4348-4439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L4348-L4439)

## Overview
Creates a NestLoop join plan node from a NestPath, implementing nested loop joins where the inner relation is scanned once for each row of the outer relation.

## Definition


## Detailed Description
This function creates a NestLoop execution plan node from a NestPath. Nested loop joins are the most basic join algorithm where for each row in the outer relation, the inner relation is scanned to find matching rows. The function handles path reparameterization to ensure proper parameter passing between outer and inner relations, manages the curOuterRels context for nested parameter handling, and processes join clauses appropriately for different join types (inner vs outer joins). It also identifies and sets up nestloop parameters that allow the inner scan to be parameterized by values from the outer relation.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and state information
- : NestPath representing the chosen nested loop join access path

## Dependencies
- Functions called/Symbols referenced:
  - [build_path_tlist](../b/build_path_tlist.md)
  - [reparameterize_path_by_child](../r/reparameterize_path_by_child.md)
  - [create_plan_recurse](create_plan_recurse.md)
  - [bms_union](../b/bms_union.md)
  - [bms_free](../b/bms_free.md)
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - IS_OUTER_JOIN
  - [extract_actual_join_clauses](../e/extract_actual_join_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [identify_current_nestloop_params](../i/identify_current_nestloop_params.md)
  - [make_nestloop](../m/make_nestloop.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_join_plan](create_join_plan.md)

## Notes and Other Information
- Nested loop joins are often used when one relation is much smaller than the other or when proper indexes exist on the inner relation
- The function carefully manages the curOuterRels context to ensure proper parameter passing between outer and inner sides
- Handles both inner and outer joins with appropriate clause extraction and processing
- Sets up nestloop parameters that allow the inner scan to be filtered based on outer relation values
- Located at src/backend/optimizer/plan/createplan.c:4348-4439
- Part of the JOIN METHODS section of the planner