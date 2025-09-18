# FetchStatementTargetList

## Location
src/backend/tcop/pquery.c: 348 - 432

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
  - GetPortalByName
  - PortalIsValid  
  - FetchPortalTargetList
  - FetchPreparedStatement
  - FetchPreparedStatementTargetList
- Called from (representative examples):
  - FetchPortalTargetList
  - CachedPlanGetTargetList

## Notes and Other Information
- The function is designed to be kept in sync with UtilityReturnsTuples
- The returned List should not be modified by callers
- Returns NIL for statements that don't have a determinable target list
- Used by both portal management code and plan cache functionality
- Located in src/backend/tcop/pquery.c:348-432