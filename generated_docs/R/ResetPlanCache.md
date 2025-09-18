# ResetPlanCache

## Location
src/backend/utils/cache/plancache.c: 2187 - 2233

## Overview
A global function that invalidates all cached plans and expressions in the plan cache.

## Definition


## Detailed Description
ResetPlanCache is a public function that performs a complete invalidation of all cached query plans and expressions. It iterates through both the saved_plan_list and cached_expression_list, marking all entries as invalid. This function is used when a system-wide change requires all cached plans to be discarded.

The function includes important safety logic to avoid invalidating certain types of statements that should remain valid even during cache resets. Specifically, it checks StmtPlanRequiresRevalidation() to skip invalidating transaction control statements (such as COMMIT, ROLLBACK) and other statements that would produce identical results if re-planned.

This conservative approach ensures that critical statements like ROLLBACK remain executable even in error conditions where plan revalidation might fail.

The function operates in two phases:
1. **Plan Source Invalidation**: Iterates through all cached plan sources, marking them invalid (except for statements that don't require revalidation)
2. **Expression Invalidation**: Iterates through all cached expressions, marking them all invalid

## Parameters / Member Variables
None - this function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach (iteration over doubly-linked lists)
  - dlist_container (container access from list nodes)
  - StmtPlanRequiresRevalidation (checks if plan needs revalidation)

- Called from (representative examples):
  - DiscardCommand (SQL DISCARD PLANS command)
  - DiscardAll (SQL DISCARD ALL command)
  - assign_session_replication_role (when session replication role changes)
  - PlanCacheSysCallback (syscache invalidation callback)

## Notes and Other Information
- This is a public function (non-static) accessible from other modules
- Uses magic numbers (CACHEDPLANSOURCE_MAGIC, CACHEDEXPR_MAGIC) for assertion checks
- Critical safety feature: preserves transaction control statements to ensure they remain usable in error conditions
- More aggressive than targeted invalidation callbacks - invalidates everything that can be safely invalidated
- Essential for commands like DISCARD PLANS that explicitly request cache clearing
- Used when system-wide changes (like replication role changes) require complete cache invalidation