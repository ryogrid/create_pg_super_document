# RevalidateCachedQuery

## Location
[src/backend/utils/cache/plancache.c:583-821](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L583-L821)

## Overview
RevalidateCachedQuery ensures the validity of cached analyzed-and-rewritten query trees by re-acquiring locks and redoing parse analysis when necessary due to schema changes or other invalidation events.

## Definition

```c
static List *
RevalidateCachedQuery(CachedPlanSource *plansource,
					  QueryEnvironment *queryEnv)
```
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
  - [ActiveSnapshotSet](../A/ActiveSnapshotSet.md)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)
  - [PushActiveSnapshot](../P/PushActiveSnapshot.md)
  - copyObject
  - [pg_analyze_and_rewrite_withcb](../p/pg_analyze_and_rewrite_withcb.md)
  - [pg_analyze_and_rewrite_fixedparams](../p/pg_analyze_and_rewrite_fixedparams.md)
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md)
  - [PlanCacheComputeResultDesc](../P/PlanCacheComputeResultDesc.md)
  - [equalRowTypes](../e/equalRowTypes.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - [FreeTupleDesc](../F/FreeTupleDesc.md)
  - AllocSetContextCreate
  - [extract_query_dependencies](../e/extract_query_dependencies.md)
  - [GetSearchPathMatcher](../G/GetSearchPathMatcher.md)
  - [MemoryContextSetParent](../M/MemoryContextSetParent.md)

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

## Simplified Source

```c
// Simplified version of RevalidateCachedQuery
static List *RevalidateCachedQuery(CachedPlanSource *plansource, QueryEnvironment *queryEnv) {
    bool snapshot_set;
    RawStmt *rawtree;
    List *tlist;

    // Skip revalidation for one-shot plans or statements that don't need it
    if (plansource->is_oneshot || !StmtPlanRequiresRevalidation(plansource)) {
        return NIL;
    }

    // Check if search path or RLS settings have changed
    if (plansource->is_valid) {
        if (!SearchPathMatchesCurrentEnvironment(plansource->search_path) ||
            (plansource->dependsOnRLS &&
             (plansource->rewriteRoleId != GetUserId() ||
              plansource->rewriteRowSecurity != row_security))) {
            plansource->is_valid = false;
        }
    }

    // Try to revalidate by acquiring locks
    if (plansource->is_valid) {
        AcquirePlannerLocks(plansource->query_list, true);

        if (plansource->is_valid) {
            return NIL; // Successfully revalidated
        }

        // Race condition occurred, release locks
        AcquirePlannerLocks(plansource->query_list, false);
    }

    // Invalidate and clean up old data
    plansource->is_valid = false;
    plansource->query_list = NIL;
    plansource->relationOids = NIL;
    plansource->invalItems = NIL;

    if (plansource->query_context) {
        MemoryContextDelete(plansource->query_context);
        plansource->query_context = NULL;
    }

    ReleaseGenericPlan(plansource);

    // Set up snapshot for parsing if needed
    snapshot_set = false;
    if (!ActiveSnapshotSet()) {
        PushActiveSnapshot(GetTransactionSnapshot());
        snapshot_set = true;
    }

    // Re-parse and rewrite the query
    rawtree = copyObject(plansource->raw_parse_tree);
    if (rawtree == NULL) {
        tlist = NIL;
    } else if (plansource->parserSetup != NULL) {
        tlist = pg_analyze_and_rewrite_withcb(rawtree, plansource->query_string,
                                            plansource->parserSetup,
                                            plansource->parserSetupArg, queryEnv);
    } else {
        tlist = pg_analyze_and_rewrite_fixedparams(rawtree, plansource->query_string,
                                                 plansource->param_types,
                                                 plansource->num_params, queryEnv);
    }

    if (snapshot_set) {
        PopActiveSnapshot();
    }

    // Update result descriptor and create new query context
    TupleDesc resultDesc = PlanCacheComputeResultDesc(tlist);
    if (resultDesc != plansource->resultDesc) {
        if (plansource->fixed_result && resultDesc != NULL) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                          errmsg("cached plan must not change result type")));
        }
        // Update result descriptor
        if (plansource->resultDesc) {
            FreeTupleDesc(plansource->resultDesc);
        }
        plansource->resultDesc = resultDesc ? CreateTupleDescCopy(resultDesc) : NULL;
    }

    // Create new query context and copy query tree
    MemoryContext querytree_context = AllocSetContextCreate(CurrentMemoryContext,
                                                           "CachedPlanQuery",
                                                           ALLOCSET_START_SMALL_SIZES);
    MemoryContext oldcxt = MemoryContextSwitchTo(querytree_context);

    List *qlist = copyObject(tlist);

    // Extract dependencies and update metadata
    extract_query_dependencies((Node *) qlist,
                              &plansource->relationOids,
                              &plansource->invalItems,
                              &plansource->dependsOnRLS);

    plansource->rewriteRoleId = GetUserId();
    plansource->rewriteRowSecurity = row_security;
    plansource->search_path = GetSearchPathMatcher(querytree_context);

    MemoryContextSwitchTo(oldcxt);

    // Finalize the new query context
    MemoryContextSetParent(querytree_context, plansource->context);
    plansource->query_context = querytree_context;
    plansource->query_list = qlist;
    plansource->is_valid = true;

    return tlist; // Return transient copy for planning
}
```

Key simplifications made:
- Consolidated error handling and validation checks into clearer logical blocks
- Removed detailed comments and merged similar operations
- Simplified memory context management while preserving essential logic
- Combined related validation checks for readability
- Focused on the main execution path while preserving all critical functionality