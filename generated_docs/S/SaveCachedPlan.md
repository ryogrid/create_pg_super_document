# SaveCachedPlan

## Location
[src/backend/utils/cache/plancache.c:482-525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L482-L525)

## Overview
SaveCachedPlan permanently saves a completed cached plan by moving it to long-lived memory and registering it for cache invalidation events.

## Definition

```c
void
SaveCachedPlan(CachedPlanSource *plansource)
```
## Detailed Description
SaveCachedPlan transitions a CachedPlanSource from transient to permanent storage by reparenting its memory context under CacheMemoryContext, making it live for the entire backend lifetime unless explicitly dropped. The function adds the plan to the global list of cached plans that are monitored for invalidation when system catalog changes occur. This is a critical step that transforms a temporary plan cache entry into a persistent one that can survive transaction boundaries.

The function is designed to be error-free (except for caller errors) since it's typically called when adding pointers to permanent data structures. Before saving, any existing generic plan is released as a safety measure since the caller may not have ensured proper reference management for long-lived contexts.

## Parameters / Member Variables
- : The completed CachedPlanSource to be saved permanently

## Dependencies
- Functions called/Symbols referenced:
  - CACHEDPLANSOURCE_MAGIC
  - [ReleaseGenericPlan](../R/ReleaseGenericPlan.md)
  - [MemoryContextSetParent](../M/MemoryContextSetParent.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - CacheMemoryContext
  - saved_plan_list

- Called from (representative examples):
  - [StorePreparedStatement](StorePreparedStatement.md) (src/backend/commands/prepare.c:420)
  - [SPI_keepplan](SPI_keepplan.md) (src/backend/executor/spi.c:996)
  - [_SPI_save_plan](_SPI_save_plan.md) (src/backend/executor/spi.c:3270)
  - [exec_parse_message](../e/exec_parse_message.md) (src/backend/tcop/postgres.c:1579)

## Notes and Other Information
- The function cannot be used with oneshot plans and will throw an ERROR if attempted
- Any existing generic plan is discarded before saving as a safety precaution
- The plan becomes subject to invalidation monitoring once saved
- Memory context reparenting ensures the plan survives transaction boundaries
- The function is guaranteed not to throw errors except for caller misuse
- Once saved, the CachedPlanSource is marked with is_saved = true flag

## Simplified Source

```c
// Simplified version of SaveCachedPlan
void SaveCachedPlan(CachedPlanSource *plansource) {
    // Validate the plan source is ready to be saved
    Assert(plansource->magic == CACHEDPLANSOURCE_MAGIC);
    Assert(plansource->is_complete);
    Assert(!plansource->is_saved);

    // Error check: Cannot save one-shot plans
    if (plansource->is_oneshot) {
        elog(ERROR, "cannot save one-shot cached plan");
    }

    // Safety measure: Release any existing generic plan
    // This prevents issues with references in long-lived contexts
    ReleaseGenericPlan(plansource);

    // Move plan to permanent memory context
    // This makes the plan live for the entire backend lifetime
    MemoryContextSetParent(plansource->context, CacheMemoryContext);

    // Add to global list for invalidation monitoring
    dlist_push_tail(&saved_plan_list, &plansource->node);

    // Mark as permanently saved
    plansource->is_saved = true;
}
```

Key simplifications made:
- Consolidated assertion checks with clear purpose comments
- Simplified the error handling explanation
- Abstracted the memory context reparenting concept
- Focused on the main execution path of validation, cleanup, reparenting, and registration
- Removed detailed implementation comments while preserving the essential logic flow