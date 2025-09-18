# checkTargetlistEntrySQL92

## Location
src/backend/parser/parse_clause.c: 1950 - 2005

## Overview
Validates a targetlist entry found by findTargetlistEntrySQL92 to ensure it is acceptable for use in specific SQL clause types like GROUP BY, ORDER BY, or DISTINCT ON.

## Definition


## Detailed Description
This function performs validation checks on a pre-existing targetlist entry that was selected using SQL92-style syntax (such as "GROUP BY 1" referring to the first column in the SELECT list). The function ensures that the selected expression is valid for use in the specified clause context. Different clause types have different restrictions:

- **GROUP BY**: Prohibits aggregate functions and window functions
- **ORDER BY**: No additional restrictions beyond basic expression validity
- **DISTINCT ON**: No additional restrictions beyond basic expression validity

The validation is necessary because when a targetlist entry is referenced by position number, the parser initially treats it as a regular targetlist item without considering the context where it will be used.

## Parameters / Member Variables
- : Parse state containing information about the current parsing context, including flags for aggregate and window function presence
- : The TargetEntry to validate, containing the expression and metadata
- : Enumeration value indicating the type of SQL clause (GROUP BY, ORDER BY, DISTINCT ON, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - contain_aggs_of_level
  - contain_windowfuncs
  - ParseExprKindName
  - locate_agg_of_level
  - locate_windowfunc
  - EXPR_KIND_GROUP_BY
  - EXPR_KIND_ORDER_BY
  - EXPR_KIND_DISTINCT_ON
- Called from (representative examples):
  - findTargetlistEntrySQL92

## Notes and Other Information
- This is a static function within parse_clause.c, indicating it's an internal helper function
- The function uses PostgreSQL's error reporting mechanism (ereport) to provide detailed error messages with proper error codes
- Error positioning information is preserved to help users identify the problematic part of their SQL query
- The validation logic is specific to SQL92 standard compliance requirements
- Window functions and aggregate functions are treated as distinct categories of prohibited expressions in GROUP BY clauses