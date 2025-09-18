# FigureColname

## Location
[src/backend/parser/parse_target.c:1704-1722](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L1704-L1722)

## Overview
Determines a suitable column name for target list entries when no explicit name is specified, following SQL specification guidance and PostgreSQL conventions.

## Definition


## Detailed Description
FigureColname serves as the main entry point for automatic column name generation in PostgreSQL's target list processing. When a SELECT list item lacks an explicit alias (AS clause), this function analyzes the untransformed parse tree to derive an appropriate column name.

The function delegates the actual work to FigureColnameInternal, which implements a sophisticated heuristic system that examines different node types to extract meaningful names. The function operates with a confidence-based approach where different naming strategies have different strength levels (0 = no information, 1 = second-best choice, 2 = good choice).

Key naming strategies include:
- **Column references**: Uses the final field name from qualified references (e.g., table.column → "column")
- **Function calls**: Uses the function name (e.g., max(value) → "max")
- **Type casts**: Prefers the underlying expression name, falls back to type name
- **Special constructs**: Provides standard names for CASE expressions, ARRAY constructs, sublinks, etc.
- **SQL functions**: Uses appropriate names for built-in SQL functions like CURRENT_DATE, COALESCE, etc.

If no suitable name can be determined, the function returns the default PostgreSQL convention "?column?".

## Parameters / Member Variables
- : The untransformed parse tree node for the target item from which to derive a column name

## Dependencies
- Functions called/Symbols referenced:
  - [FigureColnameInternal](FigureColnameInternal.md)
- Called from (representative examples):
  - [transformTargetEntry](../t/transformTargetEntry.md)
  - [transformRangeFunction](../t/transformRangeFunction.md)
  - transformXmlExpr

## Notes and Other Information
- This function works on untransformed parse trees, which is more convenient than working with transformed expressions
- The default "?column?" result is a PostgreSQL convention that clearly indicates an auto-generated name
- The function is essential for PostgreSQL's user-friendly behavior in SELECT statements without explicit column aliases
- Column name generation follows a hierarchy of preferences, with direct column references being most preferred
- The function handles a comprehensive set of PostgreSQL expression types, including modern additions like JSON functions
- Performance is generally good since it only examines the syntax tree structure rather than performing semantic analysis