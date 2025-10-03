# ResetPlanCache

## Location
[src/backend/utils/cache/plancache.c:2187-2233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L2187-L2233)

## Overview
A global function that invalidates all cached plans and expressions in the plan cache.

## Definition

```c
void
ResetPlanCache(void)
```
## Detailed Description
ResetPlanCache is a public function that performs a complete invalidation of all cached query plans and expressions. It iterates through both the saved_plan_list and cached_expression_list, marking all entries as invalid. This function is used when a system-wide change requires all cached plans to be discarded.

The function includes important safety logic to avoid invalidating certain types of statements that should remain valid even during cache resets. Specifically, it checks StmtPlanRequiresRevalidation() to skip invalidating transaction control statements (such as COMMIT, ROLLBACK) and other statements that would produce identical results if re-planned.

This conservative approach ensures that critical statements like ROLLBACK remain executable even in error conditions where plan revalidation might fail.

The function operates in two phases:
1. **Plan Source Invalidation**: Iterates through all cached plan sources, marking them invalid (except for statements that don't require revalidation)
2. **Expression Invalidation**: Iterates through all cached expressions, marking them all invalid

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach (iteration over doubly-linked lists)
  - dlist_container (container access from list nodes)
  - StmtPlanRequiresRevalidation (checks if plan needs revalidation)

- Called from (representative examples):
  - [DiscardCommand](../D/DiscardCommand.md) (SQL DISCARD PLANS command)
  - [DiscardAll](../D/DiscardAll.md) (SQL DISCARD ALL command)
  - [assign_session_replication_role](../a/assign_session_replication_role.md) (when session replication role changes)
  - [PlanCacheSysCallback](../P/PlanCacheSysCallback.md) (syscache invalidation callback)

## Notes and Other Information
- This is a public function (non-static) accessible from other modules
- Uses magic numbers (CACHEDPLANSOURCE_MAGIC, CACHEDEXPR_MAGIC) for assertion checks
- Critical safety feature: preserves transaction control statements to ensure they remain usable in error conditions
- More aggressive than targeted invalidation callbacks - invalidates everything that can be safely invalidated
- Essential for commands like DISCARD PLANS that explicitly request cache clearing
- Used when system-wide changes (like replication role changes) require complete cache invalidation

## Simplified Source

```c
// Simplified version of ResetPlanCache
void ResetPlanCache(void) {
    dlist_iter iter;

    // Phase 1: Invalidate cached plan sources
    dlist_foreach(iter, &saved_plan_list) {
        CachedPlanSource *plansource = dlist_container(CachedPlanSource, node, iter.cur);

        // Skip plans that are already invalid
        if (!plansource->is_valid)
            continue;

        // Skip transaction control statements (COMMIT, ROLLBACK, etc.)
        // These must remain valid even during cache resets for safety
        if (!StmtPlanRequiresRevalidation(plansource))
            continue;

        // Mark plan source and its generic plan as invalid
        plansource->is_valid = false;
        if (plansource->gplan)
            plansource->gplan->is_valid = false;
    }

    // Phase 2: Invalidate all cached expressions
    dlist_foreach(iter, &cached_expression_list) {
        CachedExpression *expr = dlist_container(CachedExpression, node, iter.cur);
        expr->is_valid = false;
    }
}
```

Key simplifications made:
- Removed Assert() calls for cleaner focus on core logic
- Added explanatory comments for each major phase
- Simplified variable names (cexpr -> expr)
- Consolidated the two-phase operation structure with clear comments
- Emphasized the safety logic for transaction control statements
- Removed magic number references from the simplified version