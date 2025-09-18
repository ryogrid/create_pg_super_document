# RevalidateCachedQuery

## Location
src/backend/utils/cache/plancache.c: 583 - 821

## Overview
RevalidateCachedQuery ensures the validity of cached analyzed-and-rewritten query trees by re-acquiring locks and redoing parse analysis when necessary due to schema changes or other invalidation events.

## Definition


## Detailed Description
RevalidateCachedQuery is a critical internal function that handles cache invalidation by revalidating cached query parse trees when underlying database objects have changed. The function performs several validation checks including search path changes, Row Level Security (RLS) dependency changes, and general schema invalidation. When revalidation is needed, it re-executes the complete parse analysis and rewrite phase, updating all dependent metadata including relation OIDs, invalidation items, and search paths.

The function implements a race condition-safe locking protocol and handles memory context management for the revalidated query trees. It returns a transient copy of the query trees if reanalysis was performed, allowing callers to avoid an additional copying step.

## Parameters / Member Variables
- : The CachedPlanSource containing the query to be revalidated
- : Query environment context for parse analysis operations

## Dependencies
- Functions called/Symbols referenced:
  - StmtPlanRequiresRevalidation
  - [SearchPathMatchesCurrentEnvironment](../S/SearchPathMatchesCurrentEnvironment.md)
  - [AcquirePlannerLocks](../A/AcquirePlannerLocks.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [ReleaseGenericPlan](ReleaseGenericPlan.md)
  - ActiveSnapshotSet
  - GetTransactionSnapshot
  - PushActiveSnapshot
  - copyObject
  - [pg_analyze_and_rewrite_withcb](../p/pg_analyze_and_rewrite_withcb.md)
  - [pg_analyze_and_rewrite_fixedparams](../p/pg_analyze_and_rewrite_fixedparams.md)
  - PopActiveSnapshot
  - [PlanCacheComputeResultDesc](../P/PlanCacheComputeResultDesc.md)
  - [equalRowTypes](../e/equalRowTypes.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - [FreeTupleDesc](../F/FreeTupleDesc.md)
  - AllocSetContextCreate
  - [extract_query_dependencies](../e/extract_query_dependencies.md)
  - [GetSearchPathMatcher](../G/GetSearchPathMatcher.md)
  - MemoryContextSetParent

- Called from (representative examples):
  - StmtPlanRequiresRevalidation (src/backend/utils/cache/plancache.c:103)
  - [BuildCachedPlan](../B/BuildCachedPlan.md) (src/backend/utils/cache/plancache.c:931)
  - [GetCachedPlan](../G/GetCachedPlan.md) (src/backend/utils/cache/plancache.c:1183)
  - [CachedPlanGetTargetList](../C/CachedPlanGetTargetList.md) (src/backend/utils/cache/plancache.c:1657)

## Notes and Other Information
- This is a static function internal to the plancache.c module
- Oneshot plans and plans not requiring revalidation return NIL immediately
- Implements race condition protection by acquiring locks before final validity check
- Handles search path changes and RLS dependency changes as invalidation triggers
- Creates new memory contexts for revalidated query trees while preserving cost estimates
- Returns NIL if no revalidation was needed, or transient query trees if reanalysis occurred
- Manages snapshots appropriately for parse analysis operations
- Updates result tuple descriptors and enforces fixed_result constraints
- Critical for maintaining cache coherency in the face of DDL changes