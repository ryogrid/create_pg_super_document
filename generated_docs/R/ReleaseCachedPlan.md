# ReleaseCachedPlan

## Location
[src/backend/utils/cache/plancache.c:1291-1335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1291-L1335)

## Overview
Decrements the reference count of a cached plan and frees the plan if the reference count reaches zero, providing reference-counted memory management for cached execution plans.

## Definition
```c
void ReleaseCachedPlan(CachedPlan *plan, ResourceOwner owner)
```

## Detailed Description
ReleaseCachedPlan implements reference counting for cached execution plans in PostgreSQL. When called, it decrements the reference count of the specified cached plan. If the reference count reaches zero, the function marks the plan as invalid and deallocates its memory context (unless it is a one-shot plan that does not own its context).

The function supports two modes of operation based on the owner parameter:
- When owner is provided, it assumes the reference is managed by a ResourceOwner and calls ResourceOwnerForgetPlanCacheRef to properly unregister the reference
- When owner is NULL, it handles references from persistent data structures like parent CachedPlanSource or Portal objects

## Parameters / Member Variables
- `plan`: Pointer to the CachedPlan structure whose reference count should be decremented
- `owner`: ResourceOwner managing this reference (NULL for persistent references)

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerForgetPlanCacheRef](ResourceOwnerForgetPlanCacheRef.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - CACHEDPLAN_MAGIC (validation constant)
- Called from (representative examples):
  - [ExplainExecuteQuery](../E/ExplainExecuteQuery.md)
  - [SPI_cursor_open_internal](../S/SPI_cursor_open_internal.md)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md)
  - [ReleaseGenericPlan](ReleaseGenericPlan.md)
  - [ResOwnerReleaseCachedPlan](ResOwnerReleaseCachedPlan.md)
  - [PortalReleaseCachedPlan](../P/PortalReleaseCachedPlan.md)

## Notes and Other Information
- The function includes safety checks using magic numbers to ensure plan validity
- One-shot plans are handled specially - they do not own their memory context and therefore cannot be freed by this function
- Transient references should always be protected by a ResourceOwner, while persistent references (like those in CachedPlanSource or Portal) use owner=NULL
- The function is critical for preventing memory leaks in PostgreSQL's plan caching system