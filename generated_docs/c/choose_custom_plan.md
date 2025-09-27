# choose_custom_plan

## Location
[src/backend/utils/cache/plancache.c:1046-1102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1046-L1102)

## Overview
choose_custom_plan implements the policy logic for deciding whether GetCachedPlan should use a custom parameter-specific plan or a generic parameter-independent plan based on cost analysis and configuration settings.

## Definition

```c
static bool
choose_custom_plan(CachedPlanSource *plansource, ParamListInfo boundParams)
```
## Detailed Description
choose_custom_plan is the decision engine for PostgreSQL's adaptive planning system that balances between generic plans (reusable across different parameter values) and custom plans (optimized for specific parameter values). The function implements a sophisticated cost-based heuristic that considers planning overhead, execution costs, and various configuration overrides.

The decision process follows a hierarchical approach: first checking for mandatory conditions (one-shot plans, missing parameters), then applying configuration overrides (global settings and cursor options), followed by a learning phase (first 5 executions), and finally comparing average custom plan costs against generic plan costs to make the optimal choice.

## Parameters / Member Variables
- : The CachedPlanSource containing plan metadata and cost history
- : Parameter values for the current execution (NULL indicates no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - StmtPlanRequiresRevalidation (to check if planning is needed)
  - PLAN_CACHE_MODE_FORCE_GENERIC_PLAN (global setting constant)
  - PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN (global setting constant)
  - CURSOR_OPT_GENERIC_PLAN (cursor-specific setting)
  - CURSOR_OPT_CUSTOM_PLAN (cursor-specific setting)
  - plan_cache_mode (global configuration variable)
- Called from (representative examples):
  - [GetCachedPlan](../G/GetCachedPlan.md)
  - StmtPlanRequiresRevalidation

## Notes and Other Information
- One-shot plans always return true (always use custom planning)
- Returns false immediately when no parameters are present (generic plan is identical to custom)
- Respects global plan_cache_mode settings that can force either generic or custom plans
- Supports cursor-level overrides via CURSOR_OPT_GENERIC_PLAN and CURSOR_OPT_CUSTOM_PLAN
- Implements a learning phase requiring at least 5 custom plan executions before cost comparison
- Uses average custom plan costs including planning overhead to compare against generic plan execution cost
- Generic plan cost of -1 indicates the generic plan hasn't been evaluated yet, favoring generic planning
- The cost comparison includes planning costs in custom plan costs, making the comparison fair
- This function is central to PostgreSQL's adaptive query planning performance optimization

## Simplified Source

```c
// Simplified version of choose_custom_plan
static bool choose_custom_plan(CachedPlanSource *plansource, ParamListInfo boundParams) {
    // One-shot plans always use custom planning
    if (plansource->is_oneshot) {
        return true;
    }

    // No point in custom plan without parameters or if planning is not needed
    if (boundParams == NULL || !StmtPlanRequiresRevalidation(plansource)) {
        return false;
    }

    // Check global configuration overrides
    if (plan_cache_mode == PLAN_CACHE_MODE_FORCE_GENERIC_PLAN) {
        return false;
    }
    if (plan_cache_mode == PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN) {
        return true;
    }

    // Check cursor-specific overrides
    if (plansource->cursor_options & CURSOR_OPT_GENERIC_PLAN) {
        return false;
    }
    if (plansource->cursor_options & CURSOR_OPT_CUSTOM_PLAN) {
        return true;
    }

    // Learning phase: use custom plans for first 5 executions
    if (plansource->num_custom_plans < 5) {
        return true;
    }

    // Cost-based decision: compare average custom cost vs generic cost
    double avg_custom_cost = plansource->total_custom_cost / plansource->num_custom_plans;

    // Use generic plan if it's cheaper than average custom plan
    // Note: generic_cost of -1 means not yet determined, so prefer generic
    if (plansource->generic_cost < avg_custom_cost) {
        return false;
    }

    return true; // Use custom plan if it's more cost-effective
}
```

Key simplifications made:
- Organized the decision logic into clear hierarchical blocks
- Added comments explaining the purpose of each decision point
- Simplified the cost comparison logic while preserving the essential algorithm
- Removed detailed explanatory comments and consolidated similar checks