# get_basic_select_query

## Location
src/backend/utils/adt/ruleutils.c: 5904 - 6034

## Overview
Generates the basic structure of a SELECT query string by building each clause (SELECT, DISTINCT, FROM, WHERE, GROUP BY, HAVING, WINDOW) in the proper SQL format.

## Definition
```c
static void get_basic_select_query(Query *query, deparse_context *context)
```

## Detailed Description
This function constructs a textual representation of a basic SELECT query by processing each SQL clause in order. It first checks if the query can be simplified to a VALUES clause using get_simple_values_rte(). If not, it systematically builds the query string starting with SELECT/RETURN, followed by DISTINCT clause, target list, FROM clause, WHERE clause, GROUP BY clause (including grouping sets), HAVING clause, and WINDOW clause.

The function handles various SQL features including:
- DISTINCT and DISTINCT ON clauses
- Regular and RETURN-style SELECT statements  
- GROUP BY with grouping sets and DISTINCT grouping
- Proper formatting and indentation based on context settings
- Special handling for VALUES clauses that can be simplified

## Parameters / Member Variables
- `query`: The Query structure containing the parsed SELECT statement to deparse
- `context`: The deparse_context containing formatting options, buffer, and state information

## Dependencies
- Functions called/Symbols referenced:
  - get_simple_values_rte (check for simple VALUES pattern)
  - get_values_def (generate VALUES clause)
  - get_target_list (generate SELECT target list)
  - get_from_clause (generate FROM clause)
  - get_rule_expr (generate WHERE/HAVING expressions)
  - get_rule_sortgroupclause (generate GROUP BY/DISTINCT ON items)
  - get_rule_groupingset (generate grouping sets)
  - get_rule_windowclause (generate WINDOW clause)
  - appendContextKeyword (format SQL keywords with proper spacing)
- Called from (representative examples):
  - get_select_query_def (src/backend/utils/adt/ruleutils.c:5728)

## Notes and Other Information
- Part of PostgreSQL's rule decompilation system for converting internal Query structures back to SQL text
- Handles both regular SELECT and RETURN statements (for SQL functions)
- Maintains proper SQL formatting and indentation through the deparse_context
- Optimizes simple VALUES patterns by bypassing full SELECT structure when possible
- Properly manages context state like inGroupBy flag to ensure correct expression formatting