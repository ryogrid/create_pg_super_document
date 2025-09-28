# FetchStatementTargetList

## Location
[src/backend/tcop/pquery.c:348-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/pquery.c#L348-L432)

## Overview
Extracts the query target list from a statement that returns tuples, returning NIL if the statement doesn't have a determinable target list.

## Definition
List *FetchStatementTargetList(Node *stmt)

## Detailed Description
FetchStatementTargetList is a utility function that analyzes various types of PostgreSQL statement nodes to extract their target lists - the list of columns/expressions that will be returned when the statement is executed. The function handles multiple statement types including Query nodes, PlannedStmt nodes, FetchStmt nodes, and ExecuteStmt nodes.

For Query nodes, it checks the command type and returns either the main targetList for SELECT queries or the returningList for queries with RETURNING clauses. For PlannedStmt nodes, it extracts the target list from the plan tree. For utility statements, it recursively processes the underlying utility statement. For FETCH statements, it delegates to FetchPortalTargetList, and for EXECUTE statements, it fetches the prepared statement's target list.

## Parameters / Member Variables
- stmt: A Node pointer that can be a Query, PlannedStmt, FetchStmt, or ExecuteStmt containing the statement to analyze

## Dependencies
- Functions called/Symbols referenced:
  - [GetPortalByName](../G/GetPortalByName.md)
  - PortalIsValid  
  - [FetchPortalTargetList](FetchPortalTargetList.md)
  - [FetchPreparedStatement](FetchPreparedStatement.md)
  - [FetchPreparedStatementTargetList](FetchPreparedStatementTargetList.md)
- Called from (representative examples):
  - [FetchPortalTargetList](FetchPortalTargetList.md)
  - [CachedPlanGetTargetList](../C/CachedPlanGetTargetList.md)

## Notes and Other Information
- The function is designed to be kept in sync with UtilityReturnsTuples
- The returned List should not be modified by callers
- Returns NIL for statements that don't have a determinable target list
- Used by both portal management code and plan cache functionality
- Located in src/backend/tcop/pquery.c:348-432

## Simplified Source

```c
// Simplified version of FetchStatementTargetList
List *FetchStatementTargetList(Node *stmt) {
    // Handle null input
    if (stmt == NULL) {
        return NIL;
    }

    // Handle Query nodes
    if (IsA(stmt, Query)) {
        Query *query = (Query *) stmt;

        if (query->commandType == CMD_UTILITY) {
            // Recursively process utility statement
            return FetchStatementTargetList(query->utilityStmt);
        } else {
            // For SELECT queries, return main target list
            if (query->commandType == CMD_SELECT) {
                return query->targetList;
            }
            // For queries with RETURNING, return returning list
            if (query->returningList) {
                return query->returningList;
            }
            return NIL;
        }
    }

    // Handle PlannedStmt nodes
    if (IsA(stmt, PlannedStmt)) {
        PlannedStmt *planned_stmt = (PlannedStmt *) stmt;

        if (planned_stmt->commandType == CMD_UTILITY) {
            // Recursively process utility statement
            return FetchStatementTargetList(planned_stmt->utilityStmt);
        } else {
            // Return plan tree's target list for SELECT or RETURNING queries
            if (planned_stmt->commandType == CMD_SELECT || planned_stmt->hasReturning) {
                return planned_stmt->planTree->targetlist;
            }
            return NIL;
        }
    }

    // Handle FETCH statements
    if (IsA(stmt, FetchStmt)) {
        FetchStmt *fetch_stmt = (FetchStmt *) stmt;
        Portal portal = GetPortalByName(fetch_stmt->portalname);
        return FetchPortalTargetList(portal);
    }

    // Handle EXECUTE statements
    if (IsA(stmt, ExecuteStmt)) {
        ExecuteStmt *execute_stmt = (ExecuteStmt *) stmt;
        PreparedStatement *prepared = FetchPreparedStatement(execute_stmt->name, true);
        return FetchPreparedStatementTargetList(prepared);
    }

    // Unknown statement type
    return NIL;
}
```

Key simplifications made:
- Added descriptive variable names for clarity
- Consolidated similar logic branches for Query and PlannedStmt
- Added comments explaining each statement type handling
- Removed some assertions for simplicity
- Focused on core logic: identify statement type, extract appropriate target list