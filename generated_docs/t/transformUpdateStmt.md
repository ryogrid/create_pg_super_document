# transformUpdateStmt

## Location
src/backend/parser/analyze.c: 2419 - 2484

## Overview
Transforms an UPDATE statement from the parse tree into a Query node structure, handling all aspects including WITH clauses, target relations, FROM clauses, WHERE conditions, and RETURNING clauses.

## Definition
static Query *transformUpdateStmt(ParseState *pstate, UpdateStmt *stmt)

## Detailed Description
This function performs the complete transformation of an UPDATE statement into PostgreSQL's internal Query representation. The transformation process follows a specific sequence to ensure proper name resolution and access control:

1. Sets up the basic Query structure with CMD_UPDATE command type
2. Processes any WITH clause independently to establish CTEs
3. Identifies and sets the target relation with appropriate permissions (ACL_UPDATE)
4. Handles the non-standard FROM clause with special lateral access restrictions
5. Processes WHERE clause conditions for row filtering
6. Transforms any RETURNING clause for output specification
7. Transforms the target list to match UPDATE target columns
8. Assembles the final query structure with range tables and join trees

The function includes special handling for lateral references in the FROM clause, temporarily restricting access to the target relation to prevent ambiguous references during subquery processing.

## Parameters / Member Variables
- : Parse state containing context information, namespace items, and parsing state
- : The UpdateStmt node from the parse tree containing all UPDATE statement components

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates Query node)
  - CMD_UPDATE (command type constant)
  - [transformWithClause](transformWithClause.md) (processes WITH/CTE clauses)
  - [setTargetTable](../s/setTargetTable.md) (sets up target relation with permissions)
  - ACL_UPDATE (update permission constant)
  - [transformFromClause](transformFromClause.md) (processes FROM clause)
  - [transformWhereClause](transformWhereClause.md) (processes WHERE conditions)
  - EXPR_KIND_WHERE (expression context for WHERE)
  - [transformReturningList](transformReturningList.md) (processes RETURNING clause)
  - EXPR_KIND_RETURNING (expression context for RETURNING)
  - [transformUpdateTargetList](transformUpdateTargetList.md) (processes SET clause target list)
  - [makeFromExpr](../m/makeFromExpr.md) (creates FROM clause expression tree)
  - [assign_query_collations](../a/assign_query_collations.md) (assigns collations)
- Called from (representative examples):
  - [transformStmt](transformStmt.md) (main statement transformation dispatcher)

## Notes and Other Information
The function supports PostgreSQL's non-standard FROM clause in UPDATE statements, which is maintained for compatibility with historical POSTQUEL syntax. The lateral access control mechanism ensures proper name resolution by temporarily restricting the target relation's visibility during FROM clause processing. This prevents ambiguous column references when the same table appears in both the target and FROM clauses. The transformation maintains all necessary query properties for execution planning, including sublinks and target SRFs detection.