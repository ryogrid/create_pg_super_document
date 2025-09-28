# PlanCacheObjectCallback

## Location
[src/backend/utils/cache/plancache.c:2069-2177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L2069-L2177)

## Overview
A syscache invalidation callback function that invalidates cached plans when procedure or type objects are modified.

## Definition

```c
static void
PlanCacheObjectCallback(Datum arg, int cacheid, uint32 hashvalue)
```
## Detailed Description
PlanCacheObjectCallback is a callback function registered with the syscache invalidation system for PROCOID and TYPEOID caches. It is invoked when procedures (functions) or data types are modified or dropped. The function scans through all cached plan sources and cached expressions, invalidating any that depend on the specified object.

Unlike PlanCacheRelCallback which works with relation OIDs, this function uses cache IDs and hash values to identify dependencies. It checks PlanInvalItem structures that track fine-grained dependencies on database objects. If hashvalue is 0, it invalidates all plans that depend on any object in the specified cache.

The function operates in two main phases:
1. **Plan Source Invalidation**: Iterates through saved_plan_list, checking both querytree-level and generic plan-level invalidation items
2. **Cached Expression Invalidation**: Iterates through cached_expression_list and invalidates expressions with matching dependencies

## Parameters / Member Variables
- : Datum argument passed by the callback system (unused in this function)
- : Cache identifier (PROCOID or TYPEOID) indicating which type of object was invalidated
- : Hash value of the specific object that was invalidated, or 0 to invalidate all objects in the cache

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach (iteration over doubly-linked lists)
  - dlist_container (container access from list nodes)
  - StmtPlanRequiresRevalidation (checks if plan needs revalidation)
  - lfirst (list cell access)
  - lfirst_node (typed list cell access for PlannedStmt)
  - [PlanInvalItem](PlanInvalItem.md) (structure containing cache dependency information)

- Called from (representative examples):
  - [InitPlanCache](../I/InitPlanCache.md) (registers this callback)
  - Syscache invalidation system (via callback mechanism for PROCOID/TYPEOID changes)

## Notes and Other Information
- This is a static function internal to plancache.c
- Uses magic numbers (CACHEDPLANSOURCE_MAGIC, CACHEDEXPR_MAGIC) for assertion checks
- Handles both querytree-level and generic plan-level dependencies through PlanInvalItem lists
- Skips utility statements when checking planned statement dependencies
- More fine-grained than relation-based invalidation, using hash values for specific object identification
- Essential for maintaining plan correctness when functions or types are redefined

## Simplified Source

```c
// Simplified version of PlanCacheObjectCallback
static void
PlanCacheObjectCallback(Datum arg, int cacheid, uint32 hashvalue)
{
    dlist_iter iter;

    // Phase 1: Invalidate cached plan sources
    dlist_foreach(iter, &saved_plan_list)
    {
        CachedPlanSource *plansource = dlist_container(CachedPlanSource, node, iter.cur);
        ListCell *lc;

        // Skip if already invalid or doesn't need revalidation
        if (!plansource->is_valid || !StmtPlanRequiresRevalidation(plansource))
            continue;

        // Check querytree-level dependencies
        foreach(lc, plansource->invalItems)
        {
            PlanInvalItem *item = (PlanInvalItem *) lfirst(lc);

            if (item->cacheId == cacheid &&
                (hashvalue == 0 || item->hashValue == hashvalue))
            {
                // Invalidate both querytree and generic plan
                plansource->is_valid = false;
                if (plansource->gplan)
                    plansource->gplan->is_valid = false;
                break;
            }
        }

        // Check generic plan dependencies if still valid
        if (plansource->gplan && plansource->gplan->is_valid)
        {
            foreach(lc, plansource->gplan->stmt_list)
            {
                PlannedStmt *plannedstmt = lfirst_node(PlannedStmt, lc);
                ListCell *lc3;

                if (plannedstmt->commandType == CMD_UTILITY)
                    continue; // Skip utility statements

                foreach(lc3, plannedstmt->invalItems)
                {
                    PlanInvalItem *item = (PlanInvalItem *) lfirst(lc3);

                    if (item->cacheId == cacheid &&
                        (hashvalue == 0 || item->hashValue == hashvalue))
                    {
                        // Invalidate generic plan only
                        plansource->gplan->is_valid = false;
                        break;
                    }
                }
                if (!plansource->gplan->is_valid)
                    break;
            }
        }
    }

    // Phase 2: Invalidate cached expressions
    dlist_foreach(iter, &cached_expression_list)
    {
        CachedExpression *cexpr = dlist_container(CachedExpression, node, iter.cur);
        ListCell *lc;

        // Skip if already invalid
        if (!cexpr->is_valid)
            continue;

        foreach(lc, cexpr->invalItems)
        {
            PlanInvalItem *item = (PlanInvalItem *) lfirst(lc);

            if (item->cacheId == cacheid &&
                (hashvalue == 0 || item->hashValue == hashvalue))
            {
                cexpr->is_valid = false;
                break;
            }
        }
    }
}
```

Key simplifications made:
- Removed detailed comments within loops for clarity
- Consolidated condition checks into single if statements
- Removed magic number assertions (kept essential logic only)
- Simplified variable declarations
- Added high-level phase comments for better understanding
- Maintained the essential two-phase algorithm: plan source invalidation followed by expression invalidation
- Preserved all critical logic paths and conditions