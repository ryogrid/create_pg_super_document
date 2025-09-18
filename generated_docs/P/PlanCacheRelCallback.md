# PlanCacheRelCallback

## Location
src/backend/utils/cache/plancache.c: 1985 - 2068

## Overview
A relcache invalidation callback function that invalidates cached plans when a relation is changed or dropped.

## Definition


## Detailed Description
PlanCacheRelCallback is a callback function registered with the relcache invalidation system. It is invoked whenever a relation (table, view, etc.) is modified or dropped. The function scans through all cached plan sources and cached expressions, invalidating any that depend on the specified relation. If relid is InvalidOid, it invalidates all plans that depend on any relation.

The function operates in two phases:
1. **Plan Source Invalidation**: Iterates through all cached plan sources in the saved_plan_list, checking both querytree dependencies and generic plan dependencies
2. **Cached Expression Invalidation**: Iterates through cached expressions in cached_expression_list and invalidates those that reference the affected relation

For each plan source, it performs validation checks to avoid unnecessary work, such as skipping already invalidated plans and plans that don't require revalidation.

## Parameters / Member Variables
- : Datum argument passed by the callback system (unused in this function)
- : OID of the relation that was invalidated, or InvalidOid to invalidate all relation-dependent plans

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach (iteration over doubly-linked lists)
  - dlist_container (container access from list nodes) 
  - StmtPlanRequiresRevalidation (checks if plan needs revalidation)
  - list_member_oid (checks if OID is in a list)
  - lfirst_node (list cell access for PlannedStmt)

- Called from (representative examples):
  - InitPlanCache (registers this callback)
  - Relcache invalidation system (via callback mechanism)

## Notes and Other Information
- This is a static function internal to plancache.c
- Uses magic numbers (CACHEDPLANSOURCE_MAGIC, CACHEDEXPR_MAGIC) for assertion checks
- Handles both querytree-level and generic plan-level dependencies
- Skips utility statements when checking planned statement dependencies
- Part of PostgreSQL's cache invalidation infrastructure that ensures plan consistency when database schema changes