cost_subplan

## Overview
Estimates the execution costs for SubPlan and InitPlan nodes, accounting for different sublink types and execution strategies including hash table optimization.

## Definition
```c
void cost_subplan(PlannerInfo *root, SubPlan *subplan, Plan *plan)
```

## Detailed Description
This function calculates the cost of executing a subplan, which represents a subquery that has been converted into a plan node. The cost estimation varies significantly based on the execution strategy and sublink type.

For hash table subplans (useHashTable = true), the subquery is executed once and results are stored in a hash table. The startup cost includes the full plan execution cost plus hash table loading overhead (cpu_operator_cost per tuple). Per-tuple costs only include probing the hash table, since the subquery execution is amortized.

For non-hash table subplans, the subquery may need to be re-executed or rescanned for each outer tuple. The cost estimation considers:

- **EXISTS sublinks**: Only need to fetch one tuple, so per-tuple cost is the run cost divided by the expected number of rows
- **ALL/ANY sublinks**: Assume 50% of tuples need to be examined on average, including CPU cost for row examination
- **Other sublinks**: Assume all tuples need to be examined

The function also handles startup cost accounting based on correlation and materialization. Uncorrelated subplans with materializing top nodes only pay startup cost once, while others pay it on every execution.

## Parameters / Member Variables
- `root`: PlannerInfo containing the query planning context
- `subplan`: SubPlan node being costed (updated with startup_cost and per_call_cost)
- `plan`: The actual Plan tree for the subquery

## Dependencies
- Functions called/Symbols referenced:
  - [cost_qual_eval](cost_qual_eval.md)
  - [make_ands_implicit](../m/make_ands_implicit.md)
  - [clamp_row_est](clamp_row_est.md)
  - [ExecMaterializesOutput](../E/ExecMaterializesOutput.md)
  - nodeTag
- Called from (representative examples):
  - [build_subplan](../b/build_subplan.md)
  - [SS_process_ctes](../S/SS_process_ctes.md)
  - [SS_make_initplan_from_plan](../S/SS_make_initplan_from_plan.md)

## Notes and Other Information
- Sets subplan->startup_cost and subplan->per_call_cost based on the cost analysis
- [Hash](../H/Hash.md) table strategy is a major optimization for subplans that can be executed once and reused
- Cost estimates for different sublink types reflect their typical execution patterns (EXISTS vs ALL/ANY vs others)
- Startup cost handling distinguishes between correlated and uncorrelated subplans
- The logic should align with tuple_fraction estimates used in make_subplan() for consistency
- [Hash](../H/Hash.md) table probing cost estimation is conservative and could potentially be refined
- For materialized uncorrelated subplans, startup cost is only charged once rather than per execution

## Simplified Source

```c
void
cost_subplan(PlannerInfo *root, SubPlan *subplan, Plan *plan)
{
    QualCost sp_cost;

    // Calculate cost of evaluating test expression
    cost_qual_eval(&sp_cost, make_ands_implicit((Expr *) subplan->testexpr), root);

    if (subplan->useHashTable)
    {
        // Hash table strategy: one-time execution cost
        sp_cost.startup += plan->total_cost + cpu_operator_cost * plan->plan_rows;
        // Per-tuple cost is just hash table probing
    }
    else
    {
        // Rescanning strategy: cost depends on sublink type
        Cost plan_run_cost = plan->total_cost - plan->startup_cost;

        if (subplan->subLinkType == EXISTS_SUBLINK)
        {
            // EXISTS: only need first tuple
            sp_cost.per_tuple += plan_run_cost / clamp_row_est(plan->plan_rows);
        }
        else if (subplan->subLinkType == ALL_SUBLINK ||
                 subplan->subLinkType == ANY_SUBLINK)
        {
            // ALL/ANY: assume 50% of tuples examined
            sp_cost.per_tuple += 0.50 * plan_run_cost;
            sp_cost.per_tuple += 0.50 * plan->plan_rows * cpu_operator_cost;
        }
        else
        {
            // Other sublinks: assume all tuples needed
            sp_cost.per_tuple += plan_run_cost;
        }

        // Handle startup cost based on correlation and materialization
        if (subplan->parParam == NIL && ExecMaterializesOutput(nodeTag(plan)))
            sp_cost.startup += plan->startup_cost;  // One-time cost
        else
            sp_cost.per_tuple += plan->startup_cost;  // Per-execution cost
    }

    // Store calculated costs
    subplan->startup_cost = sp_cost.startup;
    subplan->per_call_cost = sp_cost.per_tuple;
}
```