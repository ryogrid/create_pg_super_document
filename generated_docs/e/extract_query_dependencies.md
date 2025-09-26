# extract_query_dependencies

## Location
[src/backend/optimizer/plan/setrefs.c:3553-3588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L3553-L3588)

## Overview
Extracts dependencies from a rewritten but unplanned query tree, identifying relation OIDs, invalidation items, and row security status for plan cache invalidation purposes.

## Definition

```c
void
extract_query_dependencies(Node *query,
						   List **relationOids,
						   List **invalItems,
						   bool *hasRowSecurity)
```
## Detailed Description
This function analyzes a rewritten Query node or list of Query nodes to extract the same dependency information that would be collected during  processing, but without actually planning the query. It's specifically designed to support plan cache invalidation in  by identifying all database objects that an unplanned query depends on.

The function creates dummy planner state structures ( and ) and uses the existing dependency extraction machinery through . It's important to note that this function operates before , so it doesn't capture dependencies on inlined functions or elided  nodes that would appear in a fully planned query.

The function uses a clever hack where  is repurposed to collect row security flags rather than role dependencies, since this analysis occurs before role-related transformations.

## Parameters / Member Variables
- : The rewritten Query node or list of Query nodes to analyze for dependencies
- : Output parameter that receives a list of relation OIDs referenced by the query
- : Output parameter that receives a list of invalidation items (functions, operators, etc.) referenced by the query  
- : Output parameter indicating whether any Query in the tree has row security policies applied

## Dependencies
- Functions called/Symbols referenced:
  - MemSet
  - [extract_query_dependencies_walker](extract_query_dependencies_walker.md)
  - [PlannerGlobal](../P/PlannerGlobal.md) (type)
  - [PlannerInfo](../P/PlannerInfo.md) (type)
  - T_PlannerGlobal
  - T_PlannerInfo
  - NIL

- Called from (representative examples):
  - [CompleteCachedPlan](../C/CompleteCachedPlan.md) (src/backend/utils/cache/plancache.c:420)
  - [RevalidateCachedQuery](../R/RevalidateCachedQuery.md) (src/backend/utils/cache/plancache.c:773)

## Notes and Other Information
- This function is crucial for the plan cache system's ability to invalidate cached plans when underlying database objects change
- The dependency extraction is intentionally incomplete compared to full planning - it doesn't account for const-folding effects or domain elision, which is acceptable since those would only add dependencies, not remove critical ones
- The dummy planner structures allow reuse of existing dependency tracking infrastructure without full query planning overhead
- Row security detection is handled as a special case using the  flag in the global state