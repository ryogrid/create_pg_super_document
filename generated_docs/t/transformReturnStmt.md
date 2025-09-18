# transformReturnStmt

## Location
[src/backend/parser/analyze.c:2388-2418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L2388-L2418)

## Overview
Transforms a RETURN statement from the parse tree into a Query node structure suitable for execution planning and processing.

## Definition
static Query *transformReturnStmt(ParseState *pstate, ReturnStmt *stmt)

## Detailed Description
This function converts a RETURN statement (typically used in stored procedures and functions) into an internal Query representation. The transformation treats the RETURN statement as a special form of SELECT query with the isReturn flag set to true. The returned expression becomes the single target list item of the resulting query.

The function performs several key operations:
1. Creates a new Query node and marks it as a return statement
2. Transforms the return expression using the expression transformer
3. Creates a target list with the transformed expression
4. Resolves unknown types in the target list if needed
5. Copies range table and permission information from the parse state
6. Sets up query properties like sublinks, window functions, SRFs, and aggregates
7. Assigns appropriate collations to the query

This transformation allows RETURN statements to be processed through the same execution pipeline as SELECT statements while maintaining their distinct semantic meaning.

## Parameters / Member Variables
- : Parse state containing context information, range tables, and parsing flags
- : The ReturnStmt node from the parse tree containing the return expression

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates Query node)
  - CMD_SELECT (command type constant)
  - [makeTargetEntry](../m/makeTargetEntry.md) (creates target entry for return expression)
  - [transformExpr](transformExpr.md) (transforms the return expression)
  - EXPR_KIND_SELECT_TARGET (expression context constant)
  - [resolveTargetListUnknowns](../r/resolveTargetListUnknowns.md) (resolves unknown types)
  - [makeFromExpr](../m/makeFromExpr.md) (creates FROM clause expression)
  - [assign_query_collations](../a/assign_query_collations.md) (assigns collations to query)
- Called from (representative examples):
  - [transformStmt](transformStmt.md) (main statement transformation dispatcher)

## Notes and Other Information
The function specifically handles RETURN statements found in PostgreSQL stored procedures and functions. The resulting Query has its isReturn flag set to distinguish it from regular SELECT statements during execution. The function ensures proper type resolution and collation assignment, which is critical for return value handling. The transformation maintains all parse state information that might be relevant for later processing phases, including sublinks, window functions, and aggregates detection.