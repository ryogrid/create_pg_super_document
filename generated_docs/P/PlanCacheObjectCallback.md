# PlanCacheObjectCallback

## Location
[src/backend/utils/cache/plancache.c:2069-2177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L2069-L2177)

## Overview
A syscache invalidation callback function that invalidates cached plans when procedure or type objects are modified.

## Definition


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
  - PlanInvalItem (structure containing cache dependency information)

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