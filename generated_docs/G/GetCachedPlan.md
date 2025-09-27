# GetCachedPlan

## Location
[src/backend/utils/cache/plancache.c:1168-1290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1168-L1290)

## Overview
GetCachedPlan is the main interface for retrieving executable plans from the plan cache, implementing PostgreSQL's adaptive planning logic that automatically chooses between generic and custom plans based on cost analysis.

## Definition

```c
CachedPlan *
GetCachedPlan(CachedPlanSource *plansource, ParamListInfo boundParams,
              ResourceOwner owner, QueryEnvironment *queryEnv);
```
## Detailed Description
GetCachedPlan serves as the primary entry point for PostgreSQL's plan cache system, encapsulating the complex decision-making process between generic and custom plans. The function orchestrates multiple subsystems: query revalidation, plan selection heuristics, plan construction, memory management, and resource tracking. It ensures that returned plans are valid, properly locked for execution, and correctly reference-counted for memory safety.

The function implements a sophisticated adaptive strategy that can dynamically switch between generic and custom plans even after initially choosing generic planning. If a newly constructed generic plan proves inferior to the average custom plan cost, the function abandons the generic plan and creates a custom plan instead. This prevents poor-performing generic plans from being executed while maintaining the benefits of plan reuse when appropriate.

## Parameters / Member Variables
- `plansource`: The CachedPlanSource containing the prepared statement and metadata
- `paramLI`: Parameter values for custom plan generation (NULL for parameter-less queries)
- `owner`: ResourceOwner for tracking plan references (NULL if not needed, only works with saved plans)
- `queryEnv`: Query environment providing additional execution context

## Dependencies
- Functions called/Symbols referenced:
  - [RevalidateCachedQuery](../R/RevalidateCachedQuery.md) (validates and locks the underlying query tree)
  - [choose_custom_plan](../c/choose_custom_plan.md) (implements the custom vs generic decision logic)
  - [CheckCachedPlan](../C/CheckCachedPlan.md) (validates existing generic plans)
  - [BuildCachedPlan](../B/BuildCachedPlan.md) (constructs new plans)
  - [cached_plan_cost](../c/cached_plan_cost.md) (calculates plan execution costs)
  - [ReleaseGenericPlan](../R/ReleaseGenericPlan.md) (cleanup invalid generic plans)
  - [MemoryContextSetParent](../M/MemoryContextSetParent.md) (manages plan memory lifecycle)
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md), ResourceOwnerRememberPlanCacheRef (resource tracking)
  - CACHEDPLANSOURCE_MAGIC, CACHEDPLAN_MAGIC (validation constants)
- Called from (representative examples):
  - [ExecuteQuery](../E/ExecuteQuery.md) (prepared statement execution)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md) (SPI interface)
  - [exec_bind_message](../e/exec_bind_message.md) (protocol-level execution)
  - [SPI_cursor_open_internal](../S/SPI_cursor_open_internal.md) (cursor operations)

## Notes and Other Information
- Automatically increments plan reference count and registers with ResourceOwner if provided
- Supports both saved plans (persistent across transactions) and unsaved plans (session-local)
- Implements dynamic plan switching: can abandon newly created generic plans if they prove inferior
- Maintains cost statistics for both generic and custom plan usage
- Handles memory context reparenting to ensure proper plan lifecycle management
- Validates that saved plans are only used with ResourceOwner tracking
- Uses caller's memory context for any replanning work required
- Ensures execution locks are held before returning plans
- Accumulates cost statistics to improve future planning decisions
- Custom plans for saved plan sources are automatically moved to CacheMemoryContext
- The function hides the complexity of plan selection from callers who simply receive an optimal plan

## Simplified Source

```c
// Simplified version of GetCachedPlan
CachedPlan *
GetCachedPlan(CachedPlanSource *plansource, ParamListInfo boundParams,
              ResourceOwner owner, QueryEnvironment *queryEnv)
{
    CachedPlan *plan = NULL;
    List       *qlist;
    bool        customplan;

    // Validate input parameters
    Assert(plansource->magic == CACHEDPLANSOURCE_MAGIC);
    Assert(plansource->is_complete);
    if (owner && !plansource->is_saved)
        elog(ERROR, "cannot apply ResourceOwner to non-saved cached plan");

    // Step 1: Revalidate the cached query and ensure we have parse-time locks
    qlist = RevalidateCachedQuery(plansource, queryEnv);

    // Step 2: Decide whether to use a custom plan or generic plan
    customplan = choose_custom_plan(plansource, boundParams);

    // Step 3a: Handle generic plan path
    if (!customplan) {
        if (CheckCachedPlan(plansource)) {
            // Use existing valid generic plan
            plan = plansource->gplan;
        } else {
            // Build new generic plan
            plan = BuildCachedPlan(plansource, qlist, NULL, queryEnv);

            // Clean up old generic plan and link new one
            ReleaseGenericPlan(plansource);
            plansource->gplan = plan;
            plan->refcount++;

            // Set up memory context based on plan type
            if (plansource->is_saved) {
                MemoryContextSetParent(plan->context, CacheMemoryContext);
                plan->is_saved = true;
            } else {
                MemoryContextSetParent(plan->context,
                                     MemoryContextGetParent(plansource->context));
            }

            // Update cost tracking
            plansource->generic_cost = cached_plan_cost(plan, false);

            // Reconsider plan choice based on actual generic cost
            customplan = choose_custom_plan(plansource, boundParams);
            if (customplan)
                qlist = NIL; // Force query list re-copy for custom plan
        }
    }

    // Step 3b: Handle custom plan path
    if (customplan) {
        plan = BuildCachedPlan(plansource, qlist, boundParams, queryEnv);
        plansource->total_custom_cost += cached_plan_cost(plan, true);
        plansource->num_custom_plans++;
    } else {
        plansource->num_generic_plans++;
    }

    // Step 4: Set up plan reference counting and resource ownership
    if (owner)
        ResourceOwnerEnlarge(owner);
    plan->refcount++;
    if (owner)
        ResourceOwnerRememberPlanCacheRef(owner, plan);

    // Step 5: Handle memory context for saved custom plans
    if (customplan && plansource->is_saved) {
        MemoryContextSetParent(plan->context, CacheMemoryContext);
        plan->is_saved = true;
    }

    return plan;
}
```

Key simplifications made:
- Removed detailed comments and condensed logic flow into clear steps
- Abstracted complex memory management details into high-level operations
- Consolidated error handling to essential checks only
- Simplified the adaptive plan switching logic while preserving the core algorithm
- Added step-by-step comments to show the main execution phases
- Focused on the primary decision points: generic vs custom plan selection