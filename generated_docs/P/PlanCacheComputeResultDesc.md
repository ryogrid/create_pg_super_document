# PlanCacheComputeResultDesc

## Location
src/backend/utils/cache/plancache.c: 1949 - 1984

## Overview
Determines the result tuple descriptor for a list of analyzed and rewritten queries based on the portal strategy, returning NULL if no tuples will be produced.

## Definition
```c
static TupleDesc PlanCacheComputeResultDesc(List *stmt_list)
```

## Detailed Description
This function analyzes a list of statement queries to determine what type of result tuple descriptor should be expected from their execution. It uses ChoosePortalStrategy to determine the execution strategy and then extracts the appropriate tuple descriptor based on that strategy. For SELECT queries and queries with RETURNING clauses, it uses ExecCleanTypeFromTL to generate a clean tuple descriptor from the target list. For utility statements that return tuples, it delegates to UtilityTupleDescriptor. For multi-query portals that don't return tuples, it returns NULL. The resulting tuple descriptor is created in the current memory context.

## Parameters / Member Variables
- `stmt_list`: List of Query structures that have been analyzed and rewritten

## Dependencies
- Functions called/Symbols referenced:
  - ChoosePortalStrategy
  - PORTAL_ONE_SELECT
  - PORTAL_ONE_MOD_WITH
  - linitial_node
  - ExecCleanTypeFromTL
  - PORTAL_ONE_RETURNING
  - QueryListGetPrimaryStmt
  - PORTAL_UTIL_SELECT
  - UtilityTupleDescriptor
  - PORTAL_MULTI_QUERY
- Called from (representative examples):
  - CompleteCachedPlan
  - RevalidateCachedQuery

## Notes and Other Information
- The function handles different portal strategies: single SELECT, single statement with RETURNING, utility statements, and multi-query scenarios
- Returns NULL for PORTAL_MULTI_QUERY strategies as they don't produce result tuples
- The result tuple descriptor is created in the current memory context, making it the caller's responsibility to manage its lifetime
- For RETURNING queries, it specifically looks for the returningList rather than the targetList
- This function is crucial for plan caching as it allows the system to know what kind of results to expect without executing the plan