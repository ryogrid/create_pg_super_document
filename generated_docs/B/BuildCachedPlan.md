# BuildCachedPlan

## Location
[src/backend/utils/cache/plancache.c:906-1045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L906-L1045)

## Overview
BuildCachedPlan constructs a new CachedPlan from a CachedPlanSource, handling both generic parameter-independent plans and custom parameter-specific plans with appropriate memory management and dependency tracking.

## Definition


## Detailed Description
BuildCachedPlan is responsible for creating executable plans from cached plan sources in PostgreSQL's plan cache system. It can generate either generic plans (parameter-value-independent) or custom plans (parameter-specific) based on the boundParams argument. The function handles complex scenarios including query revalidation, snapshot management, memory context creation, and dependency tracking for role-based security and transaction isolation.

The function performs several critical operations: validates the query tree (revalidating if necessary), manages transaction snapshots for planning, invokes the PostgreSQL planner via pg_plan_queries, creates appropriate memory contexts for plan storage, and sets up plan metadata including role dependencies and transaction identifiers for transient plans.

## Parameters / Member Variables
- : The CachedPlanSource containing the source query and metadata
- : Pre-validated query list from RevalidateCachedQuery, or NIL to re-copy from plansource
- : Parameter values for custom plans (NULL for generic plans)
- : Query environment for additional context during planning

## Dependencies
- Functions called/Symbols referenced:
  - [RevalidateCachedQuery](../R/RevalidateCachedQuery.md) (for query tree validation)
  - copyObject (for deep copying query structures)
  - ActiveSnapshotSet, PushActiveSnapshot, PopActiveSnapshot (snapshot management)
  - [analyze_requires_snapshot](../a/analyze_requires_snapshot.md) (to determine if snapshot is needed)
  - GetTransactionSnapshot (for snapshot acquisition)
  - [pg_plan_queries](../p/pg_plan_queries.md) (core PostgreSQL planner interface)
  - AllocSetContextCreate (memory context creation)
  - MemoryContextCopyAndSetIdentifier (memory context naming)
  - [GetUserId](../G/GetUserId.md) (for role dependency tracking)
  - TransactionIdIsNormal (transaction validation)
  - CACHEDPLAN_MAGIC (plan validation magic number)
- Called from (representative examples):
  - [GetCachedPlan](../G/GetCachedPlan.md)
  - StmtPlanRequiresRevalidation

## Notes and Other Information
- Supports both generic plans (boundParams=NULL) and custom plans (with specific parameter values)
- For best performance with custom plans, PARAM_FLAG_CONST should be set on parameter values
- Automatically handles query revalidation if the plan source becomes invalid during execution
- Creates dedicated memory contexts for non-oneshot plans to manage memory lifecycle
- Tracks role dependencies through both RLS (Row Level Security) and planner-injected dependencies
- Manages transient plans that depend on current transaction state via saved_xmin
- One-shot plans reuse caller's memory context for efficiency
- Generation numbers are assigned to track plan versions and enable cache invalidation
- Plans are created in valid state and inherit oneshot status from their source