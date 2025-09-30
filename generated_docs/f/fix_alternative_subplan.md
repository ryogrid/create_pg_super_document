# fix_alternative_subplan

## Location
[src/backend/optimizer/plan/setrefs.c:2104-2159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L2104-L2159)

## Overview
A cost-based selection function that chooses the most efficient subplan from an AlternativeSubPlan during plan reference fixing, discarding unused alternatives.

## Definition
```c
static Node *fix_alternative_subplan(PlannerInfo *root, AlternativeSubPlan *asplan, double num_exec)
```

## Detailed Description
The `fix_alternative_subplan` function resolves AlternativeSubPlan nodes by selecting the most cost-effective subplan alternative based on estimated execution costs. AlternativeSubPlan nodes are created by the optimizer when multiple execution strategies are available for the same logical operation (such as different ways to execute a subquery).

The function evaluates each subplan alternative by calculating its total estimated cost using the formula: `startup_cost + num_exec * per_call_cost`. It selects the subplan with the lowest total cost. In cases of exact cost equality, it prefers the later plan in the list, which biases against fast-start subplans in favor of plans that may have better overall performance characteristics.

The function also maintains tracking information in the PlannerInfo structure, marking all considered subplans in isAltSubplan and marking the selected subplan in isUsedSubplan for later reference.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning context and subplan tracking arrays
- `asplan`: AlternativeSubPlan containing multiple subplan alternatives to choose from
- `num_exec`: Estimated number of executions for cost calculation

## Dependencies
- Functions called/Symbols referenced:
  - lfirst (extracts list cell contents)
  - [SubPlan](../S/SubPlan.md), AlternativeSubPlan, Cost (data type definitions)
  - NIL (empty list constant)
- Called from (representative examples):
  - [fix_scan_expr_mutator](fix_scan_expr_mutator.md)
  - [fix_join_expr_mutator](fix_join_expr_mutator.md)
  - [fix_upper_expr_mutator](fix_upper_expr_mutator.md)

## Notes and Other Information
- Performs cost-based optimization by comparing startup_cost + num_exec * per_call_cost for each alternative
- Tie-breaking strategy favors later plans in the list, biasing against fast-start subplans
- Maintains subplan tracking in root->isAltSubplan and root->isUsedSubplan arrays using 1-based plan_id indexing
- Does not attempt to fix up cost estimates in parent or higher-level plan nodes after selection
- Caller is responsible for recursively processing the returned subplan node
- Critical for finalizing execution plans when multiple algorithmic approaches are available
- Part of PostgreSQL's adaptive query execution strategy system

## Simplified Source

```c
// Simplified version of fix_alternative_subplan
static Node *
fix_alternative_subplan(PlannerInfo *root, AlternativeSubPlan *alt_plan,
                        double execution_count) {
    SubPlan *best_subplan = NULL;
    Cost best_cost = 0;
    ListCell *cell;

    // Evaluate each subplan alternative for lowest cost
    foreach(cell, alt_plan->subplans) {
        SubPlan *current_subplan = (SubPlan *) lfirst(cell);

        // Calculate total cost: startup + (executions * per-call)
        Cost total_cost = current_subplan->startup_cost +
                         execution_count * current_subplan->per_call_cost;

        // Keep cheapest plan (prefer later plans on ties)
        if (best_subplan == NULL || total_cost <= best_cost) {
            best_subplan = current_subplan;
            best_cost = total_cost;
        }

        // Mark this subplan as being part of alternatives
        root->isAltSubplan[current_subplan->plan_id - 1] = true;
    }

    // Mark the selected subplan as used
    root->isUsedSubplan[best_subplan->plan_id - 1] = true;

    return (Node *) best_subplan;
}
```

Key simplifications made:
- Used more descriptive variable names for clarity
- Added comments explaining the cost calculation logic
- Simplified the cost comparison and selection logic
- Focused on the core algorithm while preserving all functionality