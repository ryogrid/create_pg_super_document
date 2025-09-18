# GetCachedPlan

## Location
src/backend/utils/cache/plancache.c: 1168 - 1290

## Overview
GetCachedPlan is the main interface for retrieving executable plans from the plan cache, implementing PostgreSQL's adaptive planning logic that automatically chooses between generic and custom plans based on cost analysis.

## Definition


## Detailed Description
GetCachedPlan serves as the primary entry point for PostgreSQL's plan cache system, encapsulating the complex decision-making process between generic and custom plans. The function orchestrates multiple subsystems: query revalidation, plan selection heuristics, plan construction, memory management, and resource tracking. It ensures that returned plans are valid, properly locked for execution, and correctly reference-counted for memory safety.

The function implements a sophisticated adaptive strategy that can dynamically switch between generic and custom plans even after initially choosing generic planning. If a newly constructed generic plan proves inferior to the average custom plan cost, the function abandons the generic plan and creates a custom plan instead. This prevents poor-performing generic plans from being executed while maintaining the benefits of plan reuse when appropriate.

## Parameters / Member Variables
- : The CachedPlanSource containing the prepared statement and metadata
- : Parameter values for custom plan generation (NULL for parameter-less queries)
- : ResourceOwner for tracking plan references (NULL if not needed, only works with saved plans)
- : Query environment providing additional execution context

## Dependencies
- Functions called/Symbols referenced:
  - [RevalidateCachedQuery](../R/RevalidateCachedQuery.md) (validates and locks the underlying query tree)
  - [choose_custom_plan](../c/choose_custom_plan.md) (implements the custom vs generic decision logic)
  - [CheckCachedPlan](../C/CheckCachedPlan.md) (validates existing generic plans)
  - [BuildCachedPlan](../B/BuildCachedPlan.md) (constructs new plans)
  - [cached_plan_cost](../c/cached_plan_cost.md) (calculates plan execution costs)
  - [ReleaseGenericPlan](../R/ReleaseGenericPlan.md) (cleanup invalid generic plans)
  - MemoryContextSetParent (manages plan memory lifecycle)
  - ResourceOwnerEnlarge, ResourceOwnerRememberPlanCacheRef (resource tracking)
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