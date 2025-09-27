# BuildCachedPlan

## Location
[src/backend/utils/cache/plancache.c:906-1045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L906-L1045)

## Overview
BuildCachedPlan constructs a new CachedPlan from a CachedPlanSource, handling both generic parameter-independent plans and custom parameter-specific plans with appropriate memory management and dependency tracking.

## Definition

```c
struct within the new context.
	 */
	plan = (CachedPlan *) palloc(sizeof(CachedPlan));
```
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
  - [ActiveSnapshotSet](../A/ActiveSnapshotSet.md), PushActiveSnapshot, PopActiveSnapshot (snapshot management)
  - [analyze_requires_snapshot](../a/analyze_requires_snapshot.md) (to determine if snapshot is needed)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md) (for snapshot acquisition)
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

## Simplified Source

```c
// Simplified version of BuildCachedPlan
static CachedPlan *BuildCachedPlan(CachedPlanSource *plansource, List *qlist,
                                   ParamListInfo boundParams, QueryEnvironment *queryEnv) {
    CachedPlan *plan;
    List *plist;
    bool snapshot_set;
    bool is_transient;
    MemoryContext plan_context;

    // Revalidate query tree if necessary
    if (!plansource->is_valid) {
        qlist = RevalidateCachedQuery(plansource, queryEnv);
    }

    // Get query list - copy for regular plans, use original for oneshot
    if (qlist == NIL) {
        if (!plansource->is_oneshot) {
            qlist = copyObject(plansource->query_list);
        } else {
            qlist = plansource->query_list;
        }
    }

    // Set up snapshot for planning if needed
    snapshot_set = false;
    if (!ActiveSnapshotSet() &&
        plansource->raw_parse_tree &&
        analyze_requires_snapshot(plansource->raw_parse_tree)) {
        PushActiveSnapshot(GetTransactionSnapshot());
        snapshot_set = true;
    }

    // Generate the execution plan
    plist = pg_plan_queries(qlist, plansource->query_string,
                           plansource->cursor_options, boundParams);

    // Clean up snapshot
    if (snapshot_set) {
        PopActiveSnapshot();
    }

    // Create memory context for non-oneshot plans
    if (!plansource->is_oneshot) {
        plan_context = AllocSetContextCreate(CurrentMemoryContext,
                                           "CachedPlan",
                                           ALLOCSET_START_SMALL_SIZES);
        MemoryContextCopyAndSetIdentifier(plan_context, plansource->query_string);

        // Copy plan into new context
        MemoryContext oldcxt = MemoryContextSwitchTo(plan_context);
        plist = copyObject(plist);
        MemoryContextSwitchTo(oldcxt);
    } else {
        plan_context = CurrentMemoryContext;
    }

    // Create and initialize CachedPlan structure
    plan = (CachedPlan *) palloc(sizeof(CachedPlan));
    plan->magic = CACHEDPLAN_MAGIC;
    plan->stmt_list = plist;

    // Set up role and transaction dependencies
    plan->planRoleId = GetUserId();
    plan->dependsOnRole = plansource->dependsOnRLS;
    is_transient = false;

    // Check each statement for role dependencies and transient status
    ListCell *lc;
    foreach(lc, plist) {
        PlannedStmt *plannedstmt = lfirst_node(PlannedStmt, lc);

        if (plannedstmt->commandType == CMD_UTILITY) {
            continue;
        }

        if (plannedstmt->transientPlan) {
            is_transient = true;
        }
        if (plannedstmt->dependsOnRole) {
            plan->dependsOnRole = true;
        }
    }

    // Handle transient plans
    if (is_transient) {
        plan->saved_xmin = TransactionXmin;
    } else {
        plan->saved_xmin = InvalidTransactionId;
    }

    // Initialize plan metadata
    plan->refcount = 0;
    plan->context = plan_context;
    plan->is_oneshot = plansource->is_oneshot;
    plan->is_saved = false;
    plan->is_valid = true;
    plan->generation = ++(plansource->generation);

    return plan;
}
```

Key simplifications made:
- Removed detailed comments about race conditions and edge cases
- Consolidated memory context management into clearer blocks
- Simplified the dependency checking loop
- Streamlined snapshot management logic
- Preserved all essential functionality while improving readability