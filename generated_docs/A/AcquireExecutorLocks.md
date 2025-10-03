# AcquireExecutorLocks

## Location
[src/backend/utils/cache/plancache.c:1772-1827](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1772-L1827)

## Overview
Acquires or releases locks needed for execution of a cached plan by iterating through the statement list and locking/unlocking all relations referenced in the plan.

## Definition

```c
static void
AcquireExecutorLocks(List *stmt_list, bool acquire)
```
## Detailed Description
This function is responsible for managing locks on relations that are referenced in a cached execution plan. It traverses through a list of planned statements and either acquires or releases appropriate locks on all relations mentioned in the range table entries (RTEs) of each statement. For utility statements that contain embedded queries (like EXPLAIN), it delegates to ScanQueryForLocks to handle the locking. The function operates at the plan execution level, ensuring that all necessary table locks are held before plan execution begins or properly released afterward.

## Parameters / Member Variables
- `*stmt_list`: List of PlannedStmt structures representing the cached plan statements
- `acquire`: Boolean flag indicating whether to acquire locks (true) or release them (false)
## Dependencies
- Functions called/Symbols referenced:
  - [PlannedStmt](../P/PlannedStmt.md)
  - CMD_UTILITY
  - [UtilityContainsQuery](../U/UtilityContainsQuery.md)
  - [ScanQueryForLocks](../S/ScanQueryForLocks.md)
  - RTE_RELATION
  - RTE_SUBQUERY
  - [LockRelationOid](../L/LockRelationOid.md)
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
- Called from (representative examples):
  - StmtPlanRequiresRevalidation
  - [CheckCachedPlan](../C/CheckCachedPlan.md)

## Notes and Other Information
- The function only processes RTE_RELATION and RTE_SUBQUERY entries that have valid relation OIDs
- For utility statements, it only processes those containing embedded queries via UtilityContainsQuery
- The function doesn't actually open relations, so it won't fail if a relation has been dropped
- Lock acquisition is transient and non-conflicting in case of dropped relations
- This is a static function within the plan cache module, indicating it's an internal implementation detail

## Simplified Source

```c
// Simplified version of AcquireExecutorLocks
static void
AcquireExecutorLocks(List *stmt_list, bool acquire)
{
    ListCell *lc1;

    // Process each planned statement in the list
    foreach(lc1, stmt_list)
    {
        PlannedStmt *plannedstmt = lfirst_node(PlannedStmt, lc1);

        // Handle utility statements with embedded queries
        if (plannedstmt->commandType == CMD_UTILITY)
        {
            Query *query = UtilityContainsQuery(plannedstmt->utilityStmt);
            if (query)
                ScanQueryForLocks(query, acquire);
            continue;
        }

        // Process each relation in the range table
        ListCell *lc2;
        foreach(lc2, plannedstmt->rtable)
        {
            RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc2);

            // Only lock relations and subqueries with valid OIDs
            if (rte->rtekind == RTE_RELATION ||
                (rte->rtekind == RTE_SUBQUERY && OidIsValid(rte->relid)))
            {
                // Acquire or release the appropriate lock
                if (acquire)
                    LockRelationOid(rte->relid, rte->rellockmode);
                else
                    UnlockRelationOid(rte->relid, rte->rellockmode);
            }
        }
    }
}
```

Key simplifications made:
- Removed detailed comments about rule rewriting and lock behavior
- Consolidated the RTE type checking condition for better readability
- Simplified the nested foreach loop structure
- Added brief explanatory comments for major logic sections
- Maintained the essential lock acquisition/release logic flow