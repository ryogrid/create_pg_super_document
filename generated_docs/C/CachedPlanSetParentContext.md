# CachedPlanSetParentContext

## Location
[src/backend/utils/cache/plancache.c:1498-1535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1498-L1535)

## Overview
Moves a CachedPlanSource and its associated generic plan to a new memory context, allowing for flexible memory management of unsaved cached plans.

## Definition
```c
void CachedPlanSetParentContext(CachedPlanSource *plansource, MemoryContext newcontext)
```

## Detailed Description
CachedPlanSetParentContext relocates a CachedPlanSource to a different memory context by changing the parent context of the plansource's memory context and, if present, its generic plan's memory context. This function is essential for memory management scenarios where plans need to be moved between different memory contexts.

The function enforces several important restrictions:
- Only works with unsaved plans (saved plans must remain under CacheMemoryContext)
- Cannot be applied to one-shot plans
- Requires the plan to be complete before relocation

When relocating, the function handles the memory context hierarchy carefully:
- The plansource's main context is moved to the new parent
- The query_context (a child of plansource->context) automatically follows
- If a generic plan exists, its context is moved to be a sibling of the plansource context under the new parent

## Parameters / Member Variables
- `plansource`: The CachedPlanSource to relocate to a new memory context
- `newcontext`: The target MemoryContext that should become the new parent

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSetParent](../M/MemoryContextSetParent.md)
  - CACHEDPLANSOURCE_MAGIC
  - CACHEDPLAN_MAGIC
- Called from (representative examples):
  - [_SPI_make_plan_non_temp](../S/_SPI_make_plan_non_temp.md)

## Notes and Other Information
- Function validates that the plan is in a state that allows context changes (complete, unsaved, non-oneshot)
- The query_context automatically follows the plansource context since it's a child context
- Generic plans are maintained as siblings to the plansource context to ensure proper memory hierarchy
- Critical for SPI (Server Programming Interface) operations that need to move plans between temporary and permanent contexts
- Throws errors if applied to saved or one-shot plans, which have stricter memory management requirements
- Maintains the proper parent-child relationships in the PostgreSQL memory context tree