# PlanCacheSysCallback

## Location
[src/backend/utils/cache/plancache.c:2178-2186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L2178-L2186)

## Overview
A syscache invalidation callback function that invalidates all cached plans when other (non-relation, non-procedure/type) system catalog objects are modified.

## Definition

```c
static void
PlanCacheSysCallback(Datum arg, int cacheid, uint32 hashvalue)
```
## Detailed Description
PlanCacheSysCallback is a simple but powerful callback function registered with various syscache invalidation systems. Unlike the more targeted PlanCacheRelCallback and PlanCacheObjectCallback, this function takes a conservative approach by invalidating the entire plan cache when any system catalog change occurs that might affect cached plans.

This function serves as a catch-all for syscache invalidations that don't have more specific handlers. Rather than trying to determine which specific plans might be affected by changes to various system catalogs (such as operators, access methods, collations, etc.), it simply calls ResetPlanCache() to invalidate everything.

While this approach may seem inefficient, it ensures correctness and simplifies the invalidation logic for less common but potentially plan-affecting changes.

## Parameters / Member Variables
- `arg`: Datum argument passed by the callback system (unused in this function)
- `cacheid`: Cache identifier indicating which system catalog was invalidated (unused, since all plans are invalidated regardless)
- `hashvalue`: Hash value of the specific object that was invalidated (unused, since all plans are invalidated regardless)
## Dependencies
- Functions called/Symbols referenced:
  - [ResetPlanCache](../R/ResetPlanCache.md) (invalidates all cached plans and expressions)

- Called from (representative examples):
  - [InitPlanCache](../I/InitPlanCache.md) (registers this callback for multiple system caches)
  - Syscache invalidation system (via callback mechanism for various system catalog changes)

## Notes and Other Information
- This is a static function internal to plancache.c
- Takes a conservative 'invalidate everything' approach for simplicity and correctness
- Registered for multiple system caches that don't have specific invalidation handlers
- Much simpler than the targeted invalidation in PlanCacheRelCallback and PlanCacheObjectCallback
- Ensures plan cache consistency even for obscure system catalog changes that might affect query planning
- Trade-off between performance (invalidates more than necessary) and correctness/simplicity

## Simplified Source

```c
// Simplified version of PlanCacheSysCallback
static void
PlanCacheSysCallback(Datum arg, int cacheid, uint32 hashvalue)
{
    // Conservative approach: invalidate entire plan cache
    // when any system catalog change occurs
    ResetPlanCache();
}
```

Key simplifications made:
- Preserved the core logic: simple invalidation of all cached plans
- Function is already minimal - main logic is just calling ResetPlanCache()
- Parameters are accepted but unused (as intended by design)
- Conservative invalidation strategy ensures correctness over performance