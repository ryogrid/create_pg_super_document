# create_nestloop_plan

## Location
src/backend/optimizer/plan/createplan.c: 4348 - 4439

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
  - build_path_tlist
  - reparameterize_path_by_child
  - create_plan_recurse
  - bms_union
  - bms_free
  - order_qual_clauses
  - IS_OUTER_JOIN
  - extract_actual_join_clauses
  - extract_actual_clauses
  - replace_nestloop_params
  - identify_current_nestloop_params
  - make_nestloop
  - copy_generic_path_info
- Called from (representative examples):
  - create_join_plan

## Notes and Other Information
- Nested loop joins are often used when one relation is much smaller than the other or when proper indexes exist on the inner relation
- The function carefully manages the curOuterRels context to ensure proper parameter passing between outer and inner sides
- Handles both inner and outer joins with appropriate clause extraction and processing
- Sets up nestloop parameters that allow the inner scan to be filtered based on outer relation values
- Located at src/backend/optimizer/plan/createplan.c:4348-4439
- Part of the JOIN METHODS section of the planner