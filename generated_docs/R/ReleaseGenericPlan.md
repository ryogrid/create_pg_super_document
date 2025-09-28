# ReleaseGenericPlan

## Location
[src/backend/utils/cache/plancache.c:555-582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L555-L582)

## Overview
ReleaseGenericPlan safely releases a CachedPlanSource's generic plan by clearing the reference and decrementing the plan's reference count.

## Definition

```c
static void
ReleaseGenericPlan(CachedPlanSource *plansource)
```
## Detailed Description
ReleaseGenericPlan is a static utility function that safely releases the generic plan associated with a CachedPlanSource. The function checks if a generic plan exists, clears the CachedPlanSource's reference to it, and then calls ReleaseCachedPlan to properly decrement the plan's reference count. This ensures that the generic plan will be destroyed when no other references to it exist. The function is designed to be paranoid about potential failures in ReleaseCachedPlan by clearing the reference before calling it.

The function plays a crucial role in memory management for cached plans, particularly during plan invalidation, source destruction, or when transitioning plans to different memory contexts.

## Parameters / Member Variables
- : The CachedPlanSource whose generic plan should be released

## Dependencies
- Functions called/Symbols referenced:
  - [CachedPlan](../C/CachedPlan.md)
  - CACHEDPLAN_MAGIC
  - [ReleaseCachedPlan](ReleaseCachedPlan.md)

- Called from (representative examples):
  - StmtPlanRequiresRevalidation (src/backend/utils/cache/plancache.c:102)
  - [SaveCachedPlan](../S/SaveCachedPlan.md) (src/backend/utils/cache/plancache.c:500)
  - [DropCachedPlan](../D/DropCachedPlan.md) (src/backend/utils/cache/plancache.c:538)
  - [RevalidateCachedQuery](RevalidateCachedQuery.md) (src/backend/utils/cache/plancache.c:682)
  - [CheckCachedPlan](../C/CheckCachedPlan.md) (src/backend/utils/cache/plancache.c:884)
  - [GetCachedPlan](../G/GetCachedPlan.md) (src/backend/utils/cache/plancache.c:1201)

## Notes and Other Information
- This is a static function internal to the plancache.c module
- The function uses defensive programming by clearing the gplan reference before calling ReleaseCachedPlan
- Magic number validation ensures the plan structure is valid before release
- The function handles the case where gplan might be NULL gracefully
- Used extensively throughout the plan cache system for proper resource cleanup
- Critical for preventing memory leaks in the plan caching system

## Simplified Source

```c
// Simplified version of ReleaseGenericPlan
static void ReleaseGenericPlan(CachedPlanSource *plansource) {
    // Check if a generic plan exists
    if (plansource->gplan) {
        CachedPlan *plan = plansource->gplan;

        // Validate plan structure integrity
        Assert(plan->magic == CACHEDPLAN_MAGIC);

        // Clear reference before releasing to prevent double-free
        plansource->gplan = NULL;

        // Release the cached plan
        ReleaseCachedPlan(plan, NULL);
    }
}
```

Key simplifications made:
- Function is already very simple, minimal changes needed
- Added descriptive comments explaining each step
- Maintained the defensive programming pattern
- Preserved the magic number validation
- Focused on the essential plan release workflow