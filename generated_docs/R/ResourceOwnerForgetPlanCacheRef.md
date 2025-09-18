# ResourceOwnerForgetPlanCacheRef

## Location
[src/backend/utils/cache/plancache.c:140-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L140-L154)

## Overview
A convenience wrapper function that unregisters a CachedPlan from a ResourceOwner, removing it from the resource tracking system.

## Definition
```c
static inline void ResourceOwnerForgetPlanCacheRef(ResourceOwner owner, CachedPlan *plan)
```

## Detailed Description
This function is a thin wrapper around ResourceOwnerForget that specifically handles CachedPlan references. It unregisters a cached plan from the PostgreSQL resource management system, indicating that the caller is taking responsibility for the plan's lifecycle or that the plan is being released. This is the counterpart to ResourceOwnerRememberPlanCacheRef and is essential for proper resource management.

The function uses the same planref_resowner_desc descriptor as its counterpart to ensure consistent handling of plan cache references throughout their lifecycle.

## Parameters / Member Variables
- `owner`: The ResourceOwner that currently tracks this plan reference
- `plan`: The CachedPlan to be unregistered from the resource owner

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerForget
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - planref_resowner_desc (static descriptor)
- Called from (representative examples):
  - [ReleaseCachedPlan](ReleaseCachedPlan.md)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within plancache.c and will be inlined at call sites for performance
- Must be paired with a corresponding ResourceOwnerRememberPlanCacheRef call to maintain proper resource tracking
- Typically called when explicitly releasing a cached plan or transferring ownership responsibility
- Part of PostgreSQL's resource management system that prevents resource leaks and ensures proper cleanup