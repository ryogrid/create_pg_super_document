# ResourceOwnerRememberPlanCacheRef

## Location
[src/backend/utils/cache/plancache.c:135-139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L135-L139)

## Overview
A convenience wrapper function that registers a CachedPlan with a ResourceOwner to ensure proper cleanup when the resource owner is released.

## Definition
```c
static inline void ResourceOwnerRememberPlanCacheRef(ResourceOwner owner, CachedPlan *plan)
```

## Detailed Description
This function is a thin wrapper around ResourceOwnerRemember that specifically handles CachedPlan references. It registers a cached plan with the PostgreSQL resource management system, ensuring that the plan reference will be properly cleaned up when the associated ResourceOwner is released. This is crucial for preventing memory leaks and ensuring proper lifecycle management of cached execution plans.

The function uses the planref_resowner_desc descriptor which defines how plan cache references should be handled during resource cleanup, including the release phase (RESOURCE_RELEASE_AFTER_LOCKS) and priority (RELEASE_PRIO_PLANCACHE_REFS).

## Parameters / Member Variables
- `owner`: The ResourceOwner that will track this plan reference
- `plan`: The CachedPlan to be tracked by the resource owner

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerRemember](ResourceOwnerRemember.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - planref_resowner_desc (static descriptor)
- Called from (representative examples):
  - [GetCachedPlan](../G/GetCachedPlan.md)
  - [CachedPlanAllowsSimpleValidityCheck](../C/CachedPlanAllowsSimpleValidityCheck.md)  
  - [CachedPlanIsSimplyValid](../C/CachedPlanIsSimplyValid.md)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within plancache.c and will be inlined at call sites for performance
- Works in tandem with ResourceOwnerForgetPlanCacheRef for complete lifecycle management
- Part of PostgreSQL's resource management system that ensures cleanup even during error conditions
- The ResourceOwner system provides automatic cleanup during transaction abort or other error scenarios

## Simplified Source

```c
// Simplified version of ResourceOwnerRememberPlanCacheRef
static inline void ResourceOwnerRememberPlanCacheRef(ResourceOwner owner, CachedPlan *plan) {
    // Register the cached plan with the resource owner for automatic cleanup
    ResourceOwnerRemember(owner, PointerGetDatum(plan), &planref_resowner_desc);
}
```

Key simplifications made:
- Added clear comment explaining the purpose of the function
- Preserved the essential logic of registering the plan with the resource owner
- Maintained the inline and static qualifiers for performance