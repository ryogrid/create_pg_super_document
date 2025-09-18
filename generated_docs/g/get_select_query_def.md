# get_select_query_def

## Location
src/backend/utils/adt/ruleutils.c: 5702 - 5834

## Overview
Converts a SELECT Query parse tree back into readable SQL text by orchestrating the formatting of all SELECT statement components including WITH, FROM, WHERE, ORDER BY, LIMIT, and locking clauses.

## Definition


## Detailed Description
The  function serves as the main coordinator for reconstructing SELECT statements from PostgreSQL's internal Query representation. It handles the complete SELECT statement structure by processing components in the correct SQL order and delegating to specialized functions for each clause.

The function first processes the WITH clause if present, then determines whether to handle the query as a set operation (UNION/INTERSECT/EXCEPT) or a basic SELECT. For set operations, it calls  to handle the complex tree structure. For basic SELECT statements, it calls  to format the core SELECT components.

After handling the main query body, the function processes the remaining clauses in SQL order:
- ORDER BY clause with proper column number handling for set operations
- LIMIT/OFFSET clauses with support for both traditional LIMIT syntax and SQL standard FETCH FIRST...ROWS WITH TIES
- FOR UPDATE/SHARE locking clauses with various lock strengths and wait policies

The function sets up the deparse context with the query's target list and window clause information, which are needed by various sub-functions for proper name resolution and formatting.

## Parameters / Member Variables
- : Query parse tree representing the SELECT statement to be formatted
- : Deparse context containing output buffer, formatting flags, and namespace information

## Dependencies
- Functions called/Symbols referenced:
  - get_with_clause
  - get_setop_query
  - get_basic_select_query
  - get_rule_orderby
  - get_rule_expr
  - get_rtable_name
  - appendContextKeyword
  - quote_identifier
  - appendStringInfo
  - appendStringInfoString
  - appendStringInfoChar
  - PRETTYINDENT_STD
  - LIMIT_OPTION_WITH_TIES
  - LCS_FORKEYSHARE, LCS_FORSHARE, LCS_FORNOKEYUPDATE, LCS_FORUPDATE, LCS_NONE
  - LockWaitError, LockWaitSkip
  - RowMarkClause
- Called from (representative examples):
  - get_query_def

## Notes and Other Information
This function is the primary entry point for SELECT statement deparsing within the broader query deparsing system. It demonstrates PostgreSQL's comprehensive SELECT statement support, including advanced features like set operations, window functions (via windowClause context setup), and various locking modes with different wait policies. The function handles both simple and complex SELECT statements, properly formatting set operations where only ORDER BY and LIMIT clauses are meaningful at the top level. The locking clause processing supports all PostgreSQL lock strengths from KEY SHARE to UPDATE, along with NOWAIT and SKIP LOCKED options. The LIMIT clause processing includes support for the SQL standard FETCH FIRST syntax with WITH TIES option, showing PostgreSQL's SQL standards compliance alongside its traditional syntax.