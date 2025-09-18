# AcquirePlannerLocks

## Location
[src/backend/utils/cache/plancache.c:1828-1852](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1828-L1852)

## Overview
Acquires or releases locks needed for planning of a query tree list by delegating lock management to ScanQueryForLocks for each query in the list.

## Definition
```c
static void AcquirePlannerLocks(List *stmt_list, bool acquire)
```

## Detailed Description
This function manages locks required during the planning phase of query execution. It iterates through a list of Query structures and ensures that appropriate locks are acquired or released for all relations referenced in each query. Unlike AcquireExecutorLocks which works with planned statements, this function operates on raw query trees before planning. For utility statements, it checks if they contain embedded queries and only processes those. The actual lock management is delegated to ScanQueryForLocks, which performs the detailed analysis of each query's lock requirements.

## Parameters / Member Variables
- `stmt_list`: List of Query structures representing the queries to be planned
- `acquire`: Boolean flag indicating whether to acquire locks (true) or release them (false)

## Dependencies
- Functions called/Symbols referenced:
  - CMD_UTILITY
  - UtilityContainsQuery
  - [ScanQueryForLocks](../S/ScanQueryForLocks.md)
- Called from (representative examples):
  - StmtPlanRequiresRevalidation
  - [RevalidateCachedQuery](../R/RevalidateCachedQuery.md)

## Notes and Other Information
- This function operates at the planning stage, before queries are converted to execution plans
- It doesn't attempt to open relations directly, avoiding failures if relations have been dropped
- For utility statements, only those containing embedded queries are processed
- The function serves as a higher-level wrapper around ScanQueryForLocks for query lists
- This is a static function within the plan cache module, used internally for plan cache management