# get_rule_windowclause

## Location
src/backend/utils/adt/ruleutils.c: 6506 - 6537

## Overview
Formats and outputs the WINDOW clause of a SQL query for rule deparsing, displaying named window specifications while ignoring anonymous ones.

## Definition
```c
static void get_rule_windowclause(Query *query, deparse_context *context)
```

## Detailed Description
This function processes the window clause list from a query and generates the textual WINDOW clause for SQL output. It iterates through all window specifications but only outputs those with explicit names, skipping anonymous window specifications that are defined inline with window functions.

The function handles proper SQL formatting including:
- The WINDOW keyword with appropriate indentation
- Comma separation between multiple named windows  
- Proper quoting of window names using quote_identifier
- The AS keyword to associate names with window specifications

Anonymous windows (those with NULL names) are deliberately ignored since they appear inline in the SELECT clause rather than in the WINDOW clause.

## Parameters / Member Variables
- `query`: Query structure containing the windowClause list with window specifications
- `context`: Deparse context containing the output buffer and formatting state

## Dependencies
- Functions called/Symbols referenced:
  - appendContextKeyword (for formatted keyword output with indentation)
  - quote_identifier (for proper SQL identifier quoting)  
  - get_rule_windowspec (to format individual window specifications)
- Called from (representative examples):
  - get_basic_select_query (for generating complete SELECT statements)

## Notes and Other Information
- Static function accessible only within ruleutils.c
- Only processes named windows; anonymous windows are handled separately in SELECT clause
- Uses PostgreSQL's pretty-printing system for proper indentation
- Located at src/backend/utils/adt/ruleutils.c:6506-6537
- Part of the query reconstruction infrastructure for views and rules