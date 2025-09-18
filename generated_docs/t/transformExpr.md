# transformExpr

## Location
[src/backend/parser/parse_expr.c:120-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L120-L137)

## Overview
The  function is the main entry point for analyzing and transforming SQL expressions during query parsing, handling type checking and type casting to produce fully semantic expression trees.

## Definition


## Detailed Description
 serves as the primary interface for expression transformation in PostgreSQL's parser. It acts as a wrapper around , managing the parse state's expression kind context during the transformation process. The function ensures that the expression kind is properly set and restored, providing crucial context for the recursive expression analysis that follows. This design allows the parser to track what type of expression context it's currently processing (e.g., WHERE clause, SELECT list, etc.), which affects how certain expressions are interpreted and validated.

## Parameters / Member Variables
- : ParseState structure containing the current parsing context and state information
- : The raw expression node from the grammar parser that needs to be transformed
- : Enum value indicating the context/kind of expression being parsed (cannot be EXPR_KIND_NONE)

## Dependencies
- Functions called/Symbols referenced:
  - [transformExprRecurse](transformExprRecurse.md) (the actual recursive transformation logic)
  - [ParseExprKind](../P/ParseExprKind.md) (enum type for expression contexts)
  - EXPR_KIND_NONE (enum value used for validation)

- Called from (representative examples):
  - [cookDefault](../c/cookDefault.md) (for column default expressions)
  - [transformWhereClause](transformWhereClause.md) (for WHERE clause expressions)  
  - [transformTargetEntry](transformTargetEntry.md) (for SELECT list expressions)
  - [transformReturnStmt](transformReturnStmt.md) (for RETURN statement expressions)
  - transformFuncCall (for function call expressions)

## Notes and Other Information
- The function implements a save-and-restore pattern for the expression kind in ParseState, ensuring proper nesting of expression contexts
- Critical validation ensures exprKind is never EXPR_KIND_NONE, preventing invalid expression context states
- Used extensively throughout the parser for all types of SQL expressions across different statement contexts
- The separation between transformExpr and transformExprRecurse allows for clean context management while maintaining the recursive transformation logic