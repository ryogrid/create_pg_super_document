# transformFromClause

## Location
[src/backend/parser/parse_clause.c:114-179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L114-L179)

## Overview
Processes the FROM clause of SQL queries by transforming each FROM clause item and adding them to the query's range table, join list, and namespace for proper parsing and execution.

## Definition


## Detailed Description
The  function is a critical component of PostgreSQL's query parsing infrastructure that processes the FROM clause of SQL statements. It iterates through a list of FROM clause items (which can be RangeVars, RangeSubselects, RangeFunctions, and/or JoinExprs) and transforms each one while maintaining proper namespace management and lateral reference handling.

The function operates in two main phases:
1. **Left-to-right processing**: Each FROM clause item is transformed via , with namespace conflict checking and proper LATERAL reference state management
2. **Final namespace cleanup**: All namespace items are made unconditionally visible after processing is complete

The function assumes that the ParseState's p_rtable, p_joinlist, and p_namespace lists were initialized to NIL and will append to any existing entries, which is essential for rule processing and UPDATE/DELETE operations.

## Parameters / Member Variables
- : The current parse state containing the range table, join list, namespace, and other parsing context information
- : List of FROM clause items to be processed (RangeVars, RangeSubselects, RangeFunctions, and/or JoinExprs)

## Dependencies
- Functions called/Symbols referenced:
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md)
  - [transformFromClauseItem](transformFromClauseItem.md)
  - [checkNameSpaceConflicts](../c/checkNameSpaceConflicts.md)
  - [setNamespaceLateralState](../s/setNamespaceLateralState.md)
  - [list_concat](../l/list_concat.md)
- Called from (representative examples):
  - [transformDeleteStmt](transformDeleteStmt.md)
  - [transformSelectStmt](transformSelectStmt.md)
  - [transformUpdateStmt](transformUpdateStmt.md)
  - [transformPLAssignStmt](transformPLAssignStmt.md)
  - [transformMergeStmt](transformMergeStmt.md)

## Notes and Other Information
- Items must be processed left-to-right to properly handle LATERAL references
- The function supports incremental namespace building, allowing it to work with existing range table entries
- Namespace items are initially marked as visible only to LATERAL during processing, then made unconditionally visible at the end
- Essential for all SQL statements that include FROM clauses (SELECT, UPDATE, DELETE, MERGE, etc.)