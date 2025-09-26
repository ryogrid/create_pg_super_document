# materialize_finished_plan

## Location
[src/backend/optimizer/plan/createplan.c:6528-6568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6528-L6568)

## Overview
Adds a Material node on top of a completed plan, handling cost calculations and initPlan transfers that occur after the main planning phase.

## Definition

```c
Plan *
materialize_finished_plan(Plan *subplan)
```
## Detailed Description
This function wraps a completed plan with a Material node when materialization is needed after the main create_plan() phase. It handles several important post-planning tasks: moving any initPlans from the subplan to the new Material node (to prevent failures in SS_finalize_plan()), adjusting costs to account for initPlan cost transfers, and computing accurate cost estimates for the materialization operation.

The function includes what the code calls a "horrid kluge" - moving initPlans up to the Material node because the Material node becomes the effective top-level node for its query level. It also carefully manages cost accounting by removing initPlan costs from the subplan and adding them back to the Material node after computing the base materialization costs.

## Parameters / Member Variables
- : The completed Plan node that needs to be materialized

## Dependencies
- Functions called/Symbols referenced:
  - Cost (type for cost calculations)
  - [make_material](make_material.md)
  - [SS_compute_initplan_cost](../S/SS_compute_initplan_cost.md)
  - [cost_material](../c/cost_material.md)
  - [Memoize](../M/Memoize.md) (related type)
- Called from (representative examples):
  - [standard_planner](../s/standard_planner.md)
  - [build_subplan](../b/build_subplan.md)

## Notes and Other Information
- This function is used when Material nodes need to be added after the main planning phase
- Includes special handling for initPlans and their associated costs
- The comment suggests these use cases should eventually be refactored to work at the Path level
- Properly sets parallel execution properties (parallel_aware = false, parallel_safe inherited)
- The cost computation uses a dummy Path structure just for the cost_material() call
- Located in src/backend/optimizer/plan/createplan.c at lines 6528-6568