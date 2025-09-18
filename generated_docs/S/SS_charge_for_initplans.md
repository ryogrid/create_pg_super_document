# SS_charge_for_initplans

## Location
[src/backend/optimizer/plan/subselect.c:2134-2197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L2134-L2197)

## Overview
Adjusts Path costs and parallel safety flags to account for initialization plans (initPlans) that will be attached to the final execution plan.

## Definition
```c
void SS_charge_for_initplans(PlannerInfo *root, RelOptInfo *final_rel)
```

## Detailed Description
SS_charge_for_initplans performs cost accounting for initialization plans that have been created during the current query planning phase. InitPlans are subplans that must be executed once before the main query execution begins, typically for uncorrelated subqueries that can be evaluated independently.

The function performs several important adjustments:

1. **Cost Adjustment**: Adds the total initPlan execution cost to both startup_cost and total_cost of all paths in the relation, since initPlans must execute before the main plan can begin.

2. **Parallel Safety**: If any initPlan is parallel-unsafe, marks all paths as parallel-unsafe and removes partial paths entirely, since the presence of unsafe initPlans makes the entire query unsuitable for parallel execution.

3. **Path Management**: Handles both regular paths (pathlist) and partial paths (partial_pathlist) appropriately, either adjusting their costs or removing partial paths if parallelism is not feasible.

This separation from SS_attach_initplans allows the planner to account for initPlan costs during path comparison while deferring the actual attachment of initPlans until the final plan is created.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the current query planning state, including the init_plans list
- `final_rel`: RelOptInfo structure representing the final relation whose paths need cost adjustment

## Dependencies
- Functions called/Symbols referenced:
  - [SS_compute_initplan_cost](SS_compute_initplan_cost.md)
- Data types referenced:
  - Cost
  - [Path](../P/Path.md)
  - ListCell
- Called from (representative examples):
  - [subquery_planner](../s/subquery_planner.md)
  - [build_minmax_path](../b/build_minmax_path.md)

## Notes and Other Information
- Early return optimization: if root->init_plans is NIL, no work is needed
- The cost increment is computed once and applied to all paths for efficiency
- Parallel safety is handled conservatively - any unsafe initPlan makes the entire query parallel-unsafe
- When initPlans are parallel-unsafe, partial_pathlist is cleared and consider_parallel is set to false
- The function does not call set_cheapest() - the caller is responsible for recomputing the cheapest paths after cost adjustments
- This function is part of the broader initPlan management system in PostgreSQL query optimization
- InitPlans differ from regular SubPlans in that they execute exactly once and their results can be reused throughout the main query execution