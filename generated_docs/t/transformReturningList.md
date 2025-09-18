# transformReturningList

## Location
[src/backend/parser/analyze.c:2560-2618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L2560-L2618)

## Overview
Handles RETURNING clause transformation in INSERT, UPDATE, DELETE, and MERGE statements by converting the returning list to a proper target list with appropriate result number assignment and type resolution.

## Definition
List *transformReturningList(ParseState *pstate, List *returningList, ParseExprKind exprKind)

## Detailed Description
This function processes RETURNING clauses found in data modification statements (INSERT, UPDATE, DELETE, MERGE). The RETURNING clause allows these statements to return values from the affected rows, similar to a SELECT statement's target list. The transformation process includes:

1. Handling the empty case by returning NIL if no RETURNING clause exists
2. Managing result number assignment by saving the current p_next_resno and starting fresh from 1
3. Transforming the returning list using standard target list processing with the provided expression context
4. Validating that the transformation produces at least one column (preventing zero-column results)
5. Marking target list origins for proper column provenance tracking
6. Resolving any unknown types to text type if unknown resolution is enabled
7. Restoring the original result number state

The function ensures that RETURNING clauses behave consistently with SELECT target lists while maintaining proper numbering isolation from the main statement processing.

## Parameters / Member Variables
- : Parse state containing context information and resolution settings
- : List of ResTarget nodes representing the RETURNING clause expressions
- : Expression context kind (e.g., EXPR_KIND_RETURNING) for proper expression processing

## Dependencies
- Functions called/Symbols referenced:
  - [transformTargetList](transformTargetList.md) (standard target list transformation)
  - [exprLocation](../e/exprLocation.md) (gets expression location for error reporting)
  - [markTargetListOrigins](../m/markTargetListOrigins.md) (marks column origin information)
  - [resolveTargetListUnknowns](../r/resolveTargetListUnknowns.md) (resolves unknown types to text)
- Called from (representative examples):
  - [transformDeleteStmt](transformDeleteStmt.md) (DELETE statement with RETURNING)
  - [transformInsertStmt](transformInsertStmt.md) (INSERT statement with RETURNING)
  - [transformUpdateStmt](transformUpdateStmt.md) (UPDATE statement with RETURNING)
  - [transformMergeStmt](transformMergeStmt.md) (MERGE statement with RETURNING)

## Notes and Other Information
The function is used across all data modification statements that support RETURNING clauses, providing consistent behavior. The result number management ensures that RETURNING lists start numbering from 1, independent of the main statement's target list numbering. The validation for at least one column prevents confusing behavior when star-expansion results in zero columns. The type resolution to text for unknowns follows PostgreSQL's general approach for ambiguous types in output contexts. The function maintains proper isolation between RETURNING processing and main statement processing through careful state management.