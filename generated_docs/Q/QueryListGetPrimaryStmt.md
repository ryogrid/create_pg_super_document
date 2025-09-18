# QueryListGetPrimaryStmt

## Location
[src/backend/utils/cache/plancache.c:1753-1771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1753-L1771)

## Overview
Extracts the "primary" statement from a list of Query nodes, specifically the one marked with canSetTag, which indicates the statement that can set command tags.

## Definition
```c
static Query *QueryListGetPrimaryStmt(List *stmts)
```

## Detailed Description
QueryListGetPrimaryStmt searches through a list of Query statements to find and return the primary statement - the one that has its canSetTag flag set to true. The canSetTag flag indicates which statement in a multi-statement query is responsible for setting the command tag that will be reported to the client.

The function iterates through the statement list using a foreach loop and returns the first Query node that has canSetTag set to true. If no statement in the list has canSetTag set, the function returns NULL. While the function documentation notes that multiple statements being marked with canSetTag should not occur in current usage, if it did happen, only the first such statement would be returned.

This is a static function, meaning it's only accessible within the plancache.c file and serves as a utility function for other plan cache operations.

## Parameters / Member Variables
- `stmts`: List of Query nodes to search through for the primary statement

## Dependencies
- Functions called/Symbols referenced:
  - [List](../L/List.md) (PostgreSQL list structure)
  - [Query](Query.md) (PostgreSQL query structure)
  - foreach (PostgreSQL list iteration macro)
  - lfirst_node (macro to extract typed node from list cell)
- Called from (representative examples):
  - StmtPlanRequiresRevalidation
  - [CachedPlanGetTargetList](../C/CachedPlanGetTargetList.md)  
  - [PlanCacheComputeResultDesc](../P/PlanCacheComputeResultDesc.md)

## Notes and Other Information
- This is a static function, only accessible within plancache.c
- The canSetTag flag determines which statement sets the command tag for multi-statement queries
- Returns NULL if no primary statement is found
- In normal usage, exactly one statement should have canSetTag set
- The function performs a simple linear search through the statement list
- Located in src/backend/utils/cache/plancache.c:1753-1771