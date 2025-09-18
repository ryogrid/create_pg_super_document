# SS_attach_initplans

## Location
src/backend/optimizer/plan/subselect.c: 2239 - 2253

## Overview
Attaches initialization plans (initplans) created during the current query level to the specified plan node, which should be the topmost node for that query level.

## Definition


## Detailed Description
SS_attach_initplans is a utility function in PostgreSQL's query planner that handles the attachment of initialization plans to a plan node. InitPlans are subqueries that need to be executed before the main query execution begins, typically for correlated subqueries or expressions that need to be evaluated once per query execution.

The function simply takes all initplans accumulated in the PlannerInfo's init_plans list and attaches them to the target plan node. The design philosophy is to attach initplans to the topmost node of the query level rather than tracking their exact origin, as there's no performance benefit to placing them lower in the plan tree.

The function does not modify the plan node's cost estimates or parallel_safe flag, as these should have been accounted for earlier during plan creation through SS_charge_for_initplans or other initplan creation functions.

## Parameters / Member Variables
- : PlannerInfo structure containing the current planning context, including the accumulated initialization plans in root->init_plans
- : The target Plan node where the initplans will be attached, typically the topmost node of the current query level

## Dependencies
- Functions called/Symbols referenced:
  - (Direct field access only: root->init_plans, plan->initPlan)
- Called from (representative examples):
  - [create_plan](../c/create_plan.md) (src/backend/optimizer/plan/createplan.c:369)

## Notes and Other Information
- InitPlans can be attached to any node at or above their reference point, but the implementation chooses the topmost node for simplicity
- Cost accounting for initplans must be handled separately through SS_charge_for_initplans or similar functions
- The function is part of the subselect processing subsystem in PostgreSQL's optimizer
- Declared in src/include/optimizer/subselect.h