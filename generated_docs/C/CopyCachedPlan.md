# CopyCachedPlan

## Location
src/backend/utils/cache/plancache.c: 1536 - 1626

## Overview
Creates a complete deep copy of a CachedPlanSource, producing an unsaved, complete cached plan with all data structures duplicated in new memory contexts.

## Definition
```c
CachedPlanSource *CopyCachedPlan(CachedPlanSource *plansource)
```

## Detailed Description
CopyCachedPlan performs a comprehensive duplication of a CachedPlanSource, creating an independent copy with its own memory contexts and data structures. This function is equivalent to manually calling CreateCachedPlan followed by CompleteCachedPlan using the source plan's data.

The copying process includes:
- Creating new memory contexts for the plan source and query trees
- Deep copying the raw parse tree, query list, and all associated metadata
- Duplicating parameter information, relation OIDs, and invalidation items
- Copying search path, security settings, and cost estimation data
- Preserving validity state and generation information

The resulting copy is always marked as unsaved (regardless of the source's state) and does not include any generic plan that may exist in the source. The copy inherits the same validity state as the original.

## Parameters / Member Variables
- `plansource`: The source CachedPlanSource to copy (must be complete and not one-shot)

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - MemoryContextSwitchTo
  - MemoryContextSetIdentifier
  - copyObject
  - CreateTupleDescCopy
  - CopySearchPathMatcher
  - CACHEDPLANSOURCE_MAGIC
  - ALLOCSET_START_SMALL_SIZES
- Called from (representative examples):
  - _SPI_save_plan

## Notes and Other Information
- Cannot copy one-shot plans since parsing/planning may have modified the raw parse tree or query trees
- The copy is always created as unsaved, complete, and non-oneshot regardless of the source state
- No generic plan is copied - the new plan source will need to generate its own if needed
- Creates separate memory contexts for the plan source and query tree data to maintain proper memory management
- Preserves cost estimation data from the source, which can be valuable for planning decisions
- Critical for SPI operations that need to create persistent copies of temporary plans
- The copy maintains all security and access control information from the original
- Used primarily when converting temporary plans to saved plans in the SPI interface