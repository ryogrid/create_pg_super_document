# ReleaseGenericPlan

## Location
src/backend/utils/cache/plancache.c: 555 - 582

## Overview
ReleaseGenericPlan safely releases a CachedPlanSource's generic plan by clearing the reference and decrementing the plan's reference count.

## Definition


## Detailed Description
ReleaseGenericPlan is a static utility function that safely releases the generic plan associated with a CachedPlanSource. The function checks if a generic plan exists, clears the CachedPlanSource's reference to it, and then calls ReleaseCachedPlan to properly decrement the plan's reference count. This ensures that the generic plan will be destroyed when no other references to it exist. The function is designed to be paranoid about potential failures in ReleaseCachedPlan by clearing the reference before calling it.

The function plays a crucial role in memory management for cached plans, particularly during plan invalidation, source destruction, or when transitioning plans to different memory contexts.

## Parameters / Member Variables
- : The CachedPlanSource whose generic plan should be released

## Dependencies
- Functions called/Symbols referenced:
  - CachedPlan
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