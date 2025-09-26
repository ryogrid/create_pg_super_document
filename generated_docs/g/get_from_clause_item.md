# get_from_clause_item

## Location
[src/backend/utils/adt/ruleutils.c:12034-12324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L12034-L12324)

## Overview
Generates SQL text representation of a single FROM clause item (table, subquery, function, join, etc.) for query deparsing.

## Definition
```c
static void get_from_clause_item(Node *jtnode, Query *query, deparse_context *context)
```

## Detailed Description
This function is a core component of PostgreSQL's query deparsing system, responsible for converting internal join tree nodes back into SQL text format. It handles various types of FROM clause items including regular tables, subqueries, functions, table functions, VALUES clauses, CTEs, and complex joins.

The function operates by examining the node type and dispatching to appropriate handling logic:
- For RangeTblRef nodes, it processes individual range table entries (RTE) based on their kind (relation, subquery, function, etc.)
- For JoinExpr nodes, it recursively processes left and right join arguments and handles join conditions
- Each case generates appropriate SQL syntax including table names, aliases, join keywords, and conditions

Special handling is provided for:
- LATERAL queries with lateral keyword emission
- Function RTEs with complex ROWS FROM() syntax and UNNEST optimization
- Subqueries with proper parenthesization
- Join expressions with correct precedence and aliasing
- Column definition lists and tablesample clauses

## Parameters / Member Variables
- `jtnode`: Node pointer to the join tree node (either RangeTblRef or JoinExpr)
- `query`: Query structure containing the range table and other query information
- `context`: Deparse context containing output buffer, namespace information, and formatting options

## Dependencies
- Functions called/Symbols referenced:
  - rt_fetch
  - deparse_columns_fetch
  - [generate_relation_name](generate_relation_name.md)
  - [get_query_def](get_query_def.md)
  - [get_rule_expr_funccall](get_rule_expr_funccall.md)
  - [get_tablefunc](get_tablefunc.md)
  - [get_values_def](get_values_def.md)
  - [get_rte_alias](get_rte_alias.md)
  - [get_column_alias_list](get_column_alias_list.md)
  - [get_from_clause_coldeflist](get_from_clause_coldeflist.md)
  - [get_tablesample_def](get_tablesample_def.md)
  - [appendContextKeyword](../a/appendContextKeyword.md)
  - [get_rule_expr](get_rule_expr.md)
- Called from (representative examples):
  - [get_from_clause](get_from_clause.md)
  - [get_from_clause_item](get_from_clause_item.md) (recursive calls for join processing)

## Notes and Other Information
- This is a recursive function that calls itself when processing join expressions
- The function maintains proper SQL syntax formatting including parentheses, commas, and keywords
- Special optimization logic exists for UNNEST functions to collapse multiple UNNEST calls back to standard syntax
- Handles both pretty-printed and compact output formats based on context settings
- Critical for PostgreSQL's ability to display query plans and rules in human-readable SQL format