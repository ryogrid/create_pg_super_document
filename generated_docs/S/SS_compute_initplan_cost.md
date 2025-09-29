# SS_compute_initplan_cost

## Location
[src/backend/optimizer/plan/subselect.c:2198-2238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L2198-L2238)

## Overview
Computes the total cost and parallel safety status of a list of initialization plans (initPlans).

## Definition
```c
void SS_compute_initplan_cost(List *init_plans, Cost *initplan_cost_p, bool *unsafe_initplans_p)
```

## Detailed Description
SS_compute_initplan_cost calculates the aggregate cost and parallel safety characteristics of a collection of initPlans. InitPlans are subplans that execute exactly once, typically during query startup, to provide values that can be reused throughout the main query execution.

The function performs the following calculations:

1. **Cost Accumulation**: Sums up the startup_cost and per_call_cost for each initPlan in the list. The total represents the overhead that will be incurred when the initPlans execute.

2. **Parallel Safety Assessment**: Determines if any of the initPlans are not parallel-safe. If even one initPlan is unsafe, the entire collection is considered unsafe for parallel execution.

The function makes a conservative assumption that each initPlan executes once during plan startup. In reality, some initPlans might execute later or not at all depending on the query execution path, but this conservative estimate ensures accurate cost planning.

## Parameters / Member Variables
- `init_plans`: List of SubPlan nodes representing the initialization plans to cost
- `initplan_cost_p`: Output parameter that receives the computed total cost
- `unsafe_initplans_p`: Output parameter that receives true if any initPlan is parallel-unsafe

## Dependencies
- Functions called/Symbols referenced:
  - lfirst_node (macro)
- Data types referenced:
  - [List](../L/List.md)
  - Cost
  - [SubPlan](SubPlan.md)
  - ListCell
- Called from (representative examples):
  - [SS_charge_for_initplans](SS_charge_for_initplans.md)
  - [standard_planner](../s/standard_planner.md)
  - [materialize_finished_plan](../m/materialize_finished_plan.md)
  - [clean_up_removed_plan_level](../c/clean_up_removed_plan_level.md)

## Notes and Other Information
- The conservative assumption of "each initPlan runs once during startup" may overestimate costs but ensures safe planning
- Both startup_cost and per_call_cost are included in the total, as initPlans typically execute completely when invoked
- Parallel safety is evaluated with OR logic - any single unsafe initPlan makes the entire collection unsafe
- This function is widely used throughout the PostgreSQL optimizer for cost accounting when initPlans are moved between plan nodes or when their costs need to be factored into planning decisions
- The returned cost should typically be added to both startup_cost and total_cost of the plan node that will host the initPlans
- Part of the broader initPlan cost management system that ensures accurate query cost estimation

## Simplified Source

```c
void SS_compute_initplan_cost(List *init_plans, Cost *initplan_cost_p, bool *unsafe_initplans_p) {
    Cost initplan_cost = 0;
    bool unsafe_initplans = false;

    // Iterate through all initPlans
    foreach(lc, init_plans) {
        SubPlan *initsubplan = lfirst_node(SubPlan, lc);

        // Accumulate total cost (startup + per-call)
        initplan_cost += initsubplan->startup_cost + initsubplan->per_call_cost;

        // Check if any plan is not parallel-safe
        if (!initsubplan->parallel_safe)
            unsafe_initplans = true;
    }

    // Return results through output parameters
    *initplan_cost_p = initplan_cost;
    *unsafe_initplans_p = unsafe_initplans;
}
```