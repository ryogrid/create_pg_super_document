# ScanQueryForLocks

## Location
[src/backend/utils/cache/plancache.c:1853-1919](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1853-L1919)

## Overview
Recursively scans a single Query structure to acquire or release locks on all relations referenced within the query, including subqueries and common table expressions.

## Definition
```c
static void ScanQueryForLocks(Query *parsetree, bool acquire)
```

## Detailed Description
This function performs a comprehensive traversal of a Query structure to identify all relations that need to be locked during query planning or execution. It processes three main areas: range table entries (RTEs) in the main query, common table expressions (CTEs), and sublink subqueries. For RTE_RELATION entries, it directly locks/unlocks the relation. For RTE_SUBQUERY entries, it locks the view if it's based on a view (has a valid relid) and then recursively processes the subquery. The function uses query_tree_walker with ScanQueryWalker to handle complex sublink subqueries that aren't already covered by RTEs or CTEs.

## Parameters / Member Variables
- `parsetree`: Query structure to scan for lock requirements
- `acquire`: Boolean flag indicating whether to acquire locks (true) or release them (false)

## Dependencies
- Functions called/Symbols referenced:
  - CMD_UTILITY
  - RTE_RELATION
  - [LockRelationOid](../L/LockRelationOid.md)
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
  - RTE_SUBQUERY
  - CommonTableExpr
  - [ScanQueryWalker](ScanQueryWalker.md)
  - query_tree_walker
  - [QTW_IGNORE_RC_SUBQUERIES](../Q/QTW_IGNORE_RC_SUBQUERIES.md)
- Called from (representative examples):
  - [AcquireExecutorLocks](../A/AcquireExecutorLocks.md)
  - [AcquirePlannerLocks](../A/AcquirePlannerLocks.md)
  - [ScanQueryForLocks](ScanQueryForLocks.md) (recursive calls)
  - [ScanQueryWalker](ScanQueryWalker.md)

## Notes and Other Information
- The function asserts that it should not be called on utility commands (CMD_UTILITY)
- It handles recursive subquery processing for both subqueries in FROM clauses and CTEs
- Uses query_tree_walker with QTW_IGNORE_RC_SUBQUERIES flag to avoid double-processing RTEs and CTEs
- The function is designed to work with raw query trees and handles the complexity of nested query structures
- Supports both view-based subqueries (which have a relid) and regular subqueries

## Simplified Source

```c
static void ScanQueryForLocks(Query *parsetree, bool acquire)
{
    ListCell   *lc;

    // Should not be called on utility commands
    Assert(parsetree->commandType != CMD_UTILITY);

    // Process range table entries of current query level
    foreach(lc, parsetree->rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

        switch (rte->rtekind)
        {
            case RTE_RELATION:
                // Lock/unlock regular relations
                if (acquire)
                    LockRelationOid(rte->relid, rte->rellockmode);
                else
                    UnlockRelationOid(rte->relid, rte->rellockmode);
                break;

            case RTE_SUBQUERY:
                // Lock/unlock view if this subquery was expanded from a view
                if (OidIsValid(rte->relid))
                {
                    if (acquire)
                        LockRelationOid(rte->relid, rte->rellockmode);
                    else
                        UnlockRelationOid(rte->relid, rte->rellockmode);
                }
                // Recursively process the subquery
                ScanQueryForLocks(rte->subquery, acquire);
                break;

            default:
                // Ignore other RTE types (functions, values, etc.)
                break;
        }
    }

    // Process common table expressions (CTEs)
    foreach(lc, parsetree->cteList)
    {
        CommonTableExpr *cte = lfirst_node(CommonTableExpr, lc);
        ScanQueryForLocks(castNode(Query, cte->ctequery), acquire);
    }

    // Process sublink subqueries (excluding RTEs and CTEs already processed)
    if (parsetree->hasSubLinks)
    {
        query_tree_walker(parsetree, ScanQueryWalker,
                          (void *) &acquire,
                          QTW_IGNORE_RC_SUBQUERIES);
    }
}
```