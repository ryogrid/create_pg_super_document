# transformTargetEntry

## Location
[src/backend/parser/parse_target.c:75-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L75-L120)

## Overview
Transforms any ordinary expression-type node into a targetlist entry, used for converting parse tree expressions into target list elements for SQL queries.

## Definition

```c
TargetEntry *
transformTargetEntry(ParseState *pstate,
					 Node *node,
					 Node *expr,
					 ParseExprKind exprKind,
					 char *colname,
					 bool resjunk)
```
## Detailed Description
This function is a core component of PostgreSQL's query parser that converts parse tree nodes into TargetEntry structures. It handles the transformation of expression nodes into targetlist entries, which represent columns in the result set of a query. The function can work with either pre-transformed expressions or raw parse tree nodes that need transformation. It also handles automatic column name generation when no explicit column name is provided and manages special cases like SetToDefault nodes in UPDATE statements.

## Parameters / Member Variables
- : ParseState structure containing parser state information
- : The untransformed parse tree node for the value expression
- : The transformed expression, or NULL if transformation is needed
- : Expression kind constant (EXPR_KIND_SELECT_TARGET, etc.) indicating the context
- : The column name to be assigned, or NULL if none set yet
- : Boolean flag indicating if the target should be marked as resjunk (not wanted in final tuple)

## Dependencies
- Functions called/Symbols referenced:
  - [transformExpr](transformExpr.md)
  - [FigureColname](../F/FigureColname.md)
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - IsA (macro)
  - [SetToDefault](../S/SetToDefault.md)
  - [ParseExprKind](../P/ParseExprKind.md)
  - EXPR_KIND_UPDATE_SOURCE
- Called from (representative examples):
  - [transformTargetList](transformTargetList.md)
  - [findTargetlistEntrySQL99](../f/findTargetlistEntrySQL99.md)

## Notes and Other Information
- Exported function used by parse_clause.c for generating targetlist entries for ORDER/GROUP BY items
- Handles special case for SetToDefault nodes in UPDATE contexts
- Automatically generates column names using FigureColname when needed
- Increments p_next_resno in ParseState for assigning unique resource numbers
- Central to PostgreSQL's query transformation pipeline from parse tree to execution plan