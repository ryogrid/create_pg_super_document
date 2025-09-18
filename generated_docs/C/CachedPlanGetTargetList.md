# CachedPlanGetTargetList

## Location
src/backend/utils/cache/plancache.c: 1640 - 1676

## Overview
Returns the target list (tlist) that describes the output columns of a cached plan, ensuring the result is up-to-date by revalidating the cached query if necessary.

## Definition
```c
List *CachedPlanGetTargetList(CachedPlanSource *plansource, QueryEnvironment *queryEnv)
```

## Detailed Description
CachedPlanGetTargetList retrieves the target list for a cached plan source, which describes the output columns and their types for statements that return tuples. The function ensures the returned target list is current by calling RevalidateCachedQuery to refresh the cached query if needed. 

The function first performs sanity checks to ensure the plan source is valid and complete. For statements that don't return tuples (indicated by a NULL resultDesc), it immediately returns NIL. For statements that do return tuples, it revalidates the cached query, extracts the primary statement from the query list, and fetches its target list.

The returned target list is local storage within the cached plan and may disappear when the plan is next updated, so callers should not retain long-term references to it.

## Parameters / Member Variables
- `plansource`: Pointer to the CachedPlanSource structure containing the cached plan
- `queryEnv`: Query environment context for revalidation (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - CachedPlanSource (structure type)
  - QueryEnvironment (structure type)
  - CACHEDPLANSOURCE_MAGIC (magic number validation)
  - [RevalidateCachedQuery](../R/RevalidateCachedQuery.md) (ensures plan is current)
  - [QueryListGetPrimaryStmt](../Q/QueryListGetPrimaryStmt.md) (extracts primary statement)
  - [FetchStatementTargetList](../F/FetchStatementTargetList.md) (gets target list from statement)
- Called from (representative examples):
  - [FetchPreparedStatementTargetList](../F/FetchPreparedStatementTargetList.md)
  - [exec_describe_statement_message](../e/exec_describe_statement_message.md)

## Notes and Other Information
- The function assumes that whether a statement returns tuples cannot be changed by invalidation
- Includes assertions to verify the plan source magic number and completeness
- Returns NIL for statements that don't produce output tuples
- The target list is guaranteed to be up-to-date at the time of the call
- Located in src/backend/utils/cache/plancache.c:1640-1676