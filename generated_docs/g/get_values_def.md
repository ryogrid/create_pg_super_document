# get_values_def

## Location
[src/backend/utils/adt/ruleutils.c:5520-5562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L5520-L5562)

## Overview
Converts a VALUES clause from internal representation back to readable SQL text format by iterating through value lists and formatting them properly.

## Definition


## Detailed Description
The  function is responsible for reconstructing VALUES clauses from PostgreSQL's internal parse tree representation back into standard SQL syntax. It handles the formatting of multiple value lists, where each list represents a row of values, and each value within a list represents a column value.

The function operates by iterating through a list of value lists (representing rows) and for each row, iterating through the individual column values. It properly formats the output with commas separating rows and values, parentheses around each row, and the "VALUES" keyword at the beginning. The actual formatting of individual expressions is delegated to , which handles various node types including special cases like whole-row variables.

The output follows standard SQL VALUES syntax: 

## Parameters / Member Variables
- : List of Lists, where each inner List contains Node pointers representing the values in one row of the VALUES clause
- : Deparse context containing the output buffer and formatting information

## Dependencies
- Functions called/Symbols referenced:
  - get_rule_expr_toplevel
  - appendStringInfoString
  - appendStringInfoChar
  - lfirst
  - foreach
- Called from (representative examples):
  - [get_basic_select_query](get_basic_select_query.md)
  - [get_insert_query_def](get_insert_query_def.md)
  - get_from_clause_item

## Notes and Other Information
This function is specifically designed to handle VALUES clauses that can appear in various SQL contexts, including INSERT statements (INSERT INTO table VALUES ...), SELECT statements (SELECT * FROM (VALUES ...) AS t), and other contexts where VALUES is used as a table expression. The function maintains proper comma placement and parentheses structure to ensure syntactically correct SQL output. The use of  ensures that complex expressions within VALUES clauses are properly formatted, including handling of special PostgreSQL-specific constructs like whole-row variables.