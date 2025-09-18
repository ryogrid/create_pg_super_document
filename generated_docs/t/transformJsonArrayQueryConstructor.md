# transformJsonArrayQueryConstructor

## Location
[src/backend/parser/parse_expr.c:3751-3821](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L3751-L3821)

## Overview
Transforms JSON_ARRAY(query) constructor expressions into equivalent JSON_ARRAYAGG subquery expressions for PostgreSQL's JSON array construction from query results.

## Definition


## Detailed Description
This function transforms JSON_ARRAY(query [FORMAT] [RETURNING] [ON NULL]) syntax into an equivalent subquery expression using JSON_ARRAYAGG. The transformation converts the original query-based JSON array constructor into:


The transformation process involves several steps:
1. Validating that the input query returns exactly one column
2. Creating a subquery structure with proper aliasing (table alias 'q', column alias 'a')
3. Constructing a JsonArrayAgg node that aggregates the query results
4. Building a complete SELECT statement with the aggregation as the target
5. Wrapping everything in a SubLink expression for execution

The function ensures proper error handling for queries that return multiple columns, as JSON_ARRAY can only construct arrays from single-column results.

## Parameters / Member Variables
- : ParseState pointer containing current parsing context and state information for the transformation
- : JsonArrayQueryConstructor pointer containing the source JSON_ARRAY(query) constructor expression to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating various AST nodes: SubLink, SelectStmt, RangeSubselect, Alias, ResTarget, JsonArrayAgg, ColumnRef, JsonAggConstructor)
  - [make_parsestate](../m/make_parsestate.md)/free_parsestate (for temporary parsing context management)
  - [transformStmt](transformStmt.md) (for transforming the inner query)
  - copyObject (for creating a copy of the query for validation)
  - [count_nonjunk_tlist_entries](../c/count_nonjunk_tlist_entries.md) (for validating single-column requirement)
  - [makeJsonValueExpr](../m/makeJsonValueExpr.md) (for creating JSON value expressions)
  - list_make1/list_make2 (for creating lists)
  - [makeString](../m/makeString.md)/pstrdup (for string manipulation)
  - [transformExprRecurse](transformExprRecurse.md) (for final expression transformation)
  - EXPR_SUBLINK (sublink type constant)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md) (main expression transformation dispatcher)

## Notes and Other Information
- The function enforces that the input query must return exactly one column, raising a syntax error otherwise
- The generated subquery uses fixed aliases: 'q' for the table and 'a' for the column
- Format specifications and null handling options are preserved from the original constructor
- The transformation creates a complete executable subquery that can be processed by the query executor
- All location information is preserved for accurate error reporting
- The function handles both formatted and unformatted JSON value expressions appropriately