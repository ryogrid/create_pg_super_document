# get_with_clause

## Location
src/backend/utils/adt/ruleutils.c: 5563 - 5701

## Overview
Reconstructs the WITH clause (Common Table Expressions/CTEs) from internal representation back to readable SQL text, including support for recursive CTEs and advanced features like SEARCH and CYCLE clauses.

## Definition


## Detailed Description
The  function is responsible for converting PostgreSQL's internal representation of WITH clauses (Common Table Expressions) back into standard SQL syntax. It handles both non-recursive and recursive CTEs, along with PostgreSQL's advanced CTE features including materialization hints, SEARCH clauses for controlling recursive traversal order, and CYCLE clauses for cycle detection in recursive queries.

The function processes each CTE in the query's cteList, formatting them with proper syntax including:
- CTE name and optional column aliases
- MATERIALIZED/NOT MATERIALIZED hints when specified
- The actual CTE query definition
- SEARCH clauses (BREADTH FIRST or DEPTH FIRST traversal)
- CYCLE clauses for cycle detection with custom mark values

For recursive CTEs, it uses "WITH RECURSIVE" instead of just "WITH". The function handles proper indentation and formatting according to the pretty-printing flags, and calls  recursively to format the nested query definitions within each CTE.

## Parameters / Member Variables
- : Query object containing the CTE list and recursion flag
- : Deparse context containing output buffer and formatting parameters

## Dependencies
- Functions called/Symbols referenced:
  - [quote_identifier](../q/quote_identifier.md)
  - [get_query_def](get_query_def.md)
  - get_rule_expr
  - appendContextKeyword
  - appendStringInfoString
  - appendStringInfoChar
  - appendStringInfo
  - PRETTY_INDENT
  - PRETTYINDENT_STD
  - CTEMaterializeDefault, CTEMaterializeAlways, CTEMaterializeNever
  - CommonTableExpr
  - castNode
  - [DatumGetBool](../D/DatumGetBool.md)
- Called from (representative examples):
  - [get_select_query_def](get_select_query_def.md)
  - [get_insert_query_def](get_insert_query_def.md)
  - [get_update_query_def](get_update_query_def.md)
  - [get_delete_query_def](get_delete_query_def.md)
  - [get_merge_query_def](get_merge_query_def.md)

## Notes and Other Information
This function implements support for PostgreSQL's comprehensive CTE feature set, including SQL:1999 standard recursive CTEs and PostgreSQL-specific extensions. The SEARCH clause allows controlling traversal order in recursive queries (breadth-first vs depth-first), while the CYCLE clause enables automatic cycle detection with customizable mark values. The materialization hints control PostgreSQL's query optimizer behavior for CTE evaluation. The function carefully handles proper SQL syntax generation, including comma separation between multiple CTEs and correct parentheses placement around nested queries and column lists.