# PlanCacheRelCallback

## Location
[src/backend/utils/cache/plancache.c:1985-2068](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1985-L2068)

## Overview
A relcache invalidation callback function that invalidates cached plans when a relation is changed or dropped.

## Definition

```c
static void
PlanCacheRelCallback(Datum arg, Oid relid)
```
## Detailed Description
PlanCacheRelCallback is a callback function registered with the relcache invalidation system. It is invoked whenever a relation (table, view, etc.) is modified or dropped. The function scans through all cached plan sources and cached expressions, invalidating any that depend on the specified relation. If relid is InvalidOid, it invalidates all plans that depend on any relation.

The function operates in two phases:
1. **Plan Source Invalidation**: Iterates through all cached plan sources in the saved_plan_list, checking both querytree dependencies and generic plan dependencies
2. **Cached Expression Invalidation**: Iterates through cached expressions in cached_expression_list and invalidates those that reference the affected relation

For each plan source, it performs validation checks to avoid unnecessary work, such as skipping already invalidated plans and plans that don't require revalidation.

## Parameters / Member Variables
- `arg`: Datum argument passed by the callback system (unused in this function)
- `relid`: OID of the relation that was invalidated, or InvalidOid to invalidate all relation-dependent plans
## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach (iteration over doubly-linked lists)
  - dlist_container (container access from list nodes) 
  - StmtPlanRequiresRevalidation (checks if plan needs revalidation)
  - [list_member_oid](../l/list_member_oid.md) (checks if OID is in a list)
  - lfirst_node (list cell access for PlannedStmt)

- Called from (representative examples):
  - [InitPlanCache](../I/InitPlanCache.md) (registers this callback)
  - Relcache invalidation system (via callback mechanism)

## Notes and Other Information
- This is a static function internal to plancache.c
- Uses magic numbers (CACHEDPLANSOURCE_MAGIC, CACHEDEXPR_MAGIC) for assertion checks
- Handles both querytree-level and generic plan-level dependencies
- Skips utility statements when checking planned statement dependencies
- Part of PostgreSQL's cache invalidation infrastructure that ensures plan consistency when database schema changes

## Simplified Source

```c
// Simplified version of PlanCacheRelCallback
static void
PlanCacheRelCallback(Datum arg, Oid relid)
{
    dlist_iter iter;

    // Phase 1: Invalidate cached plan sources
    dlist_foreach(iter, &saved_plan_list)
    {
        CachedPlanSource *plansource = dlist_container(CachedPlanSource, node, iter.cur);

        // Skip if already invalid or doesn't need revalidation
        if (!plansource->is_valid || !StmtPlanRequiresRevalidation(plansource))
            continue;

        // Check if plan depends on the invalidated relation
        bool depends_on_rel = (relid == InvalidOid) ?
            (plansource->relationOids != NIL) :
            list_member_oid(plansource->relationOids, relid);

        if (depends_on_rel)
        {
            // Invalidate both querytree and generic plan
            plansource->is_valid = false;
            if (plansource->gplan)
                plansource->gplan->is_valid = false;
        }

        // Check generic plan dependencies separately (may have additional deps)
        if (plansource->gplan && plansource->gplan->is_valid)
        {
            foreach(lc, plansource->gplan->stmt_list)
            {
                PlannedStmt *plannedstmt = lfirst_node(PlannedStmt, lc);

                // Skip utility statements
                if (plannedstmt->commandType == CMD_UTILITY)
                    continue;

                // Check if planned statement depends on relation
                if ((relid == InvalidOid) ? (plannedstmt->relationOids != NIL) :
                    list_member_oid(plannedstmt->relationOids, relid))
                {
                    plansource->gplan->is_valid = false;
                    break;
                }
            }
        }
    }

    // Phase 2: Invalidate cached expressions
    dlist_foreach(iter, &cached_expression_list)
    {
        CachedExpression *cexpr = dlist_container(CachedExpression, node, iter.cur);

        // Skip if already invalid
        if (!cexpr->is_valid)
            continue;

        // Check if expression depends on the invalidated relation
        if ((relid == InvalidOid) ? (cexpr->relationOids != NIL) :
            list_member_oid(cexpr->relationOids, relid))
        {
            cexpr->is_valid = false;
        }
    }
}
```

Key simplifications made:
- Removed magic number assertions for clarity
- Consolidated relation dependency checks into clearer boolean variables
- Added descriptive comments for the two main phases
- Simplified the nested conditional logic while preserving the core algorithm
- Made the overall flow more readable while maintaining all essential functionality