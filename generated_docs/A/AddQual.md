# AddQual

## Location
src/backend/rewrite/rewriteManip.c: 1057 - 1124

## Overview
Adds a qualifier condition to a query's WHERE clause, with special handling for utility statements and set operations, commonly used in rule processing and query rewriting.

## Definition


## Detailed Description
This function safely adds a qualification condition to the WHERE clause of a query. It handles various edge cases and query types:

**Utility Statements**: For utility commands like NOTIFY, the function silently ignores the qualifier since there's no meaningful WHERE clause. For other utility statements, it raises an error since conditional execution is typically not desired or supported.

**Set Operations**: For UNION/INTERSECT/EXCEPT queries, the function raises an error because there's no appropriate place to add the qualifier condition in the current implementation.

**Regular Queries**: For standard DML queries, the function:
1. Creates a copy of the qualifier condition using copyObject
2. Combines it with any existing WHERE clause using make_and_qual
3. Validates that no aggregates were inadvertently added to the WHERE clause
4. Updates the query's hasSubLinks flag if the added qualifier contains subqueries

The function is primarily used during rule processing where additional conditions need to be dynamically added to queries, such as when applying rule qualifiers or when rewriting target views.

## Parameters / Member Variables
- : The Query structure to modify
- : The qualification condition (Node) to add to the WHERE clause; if NULL, the function returns without making changes

## Dependencies
- Functions called/Symbols referenced:
  - CMD_UTILITY (command type constant)
  - NotifyStmt (utility statement type)
  - copyObject (deep copy function for parse tree nodes)
  - make_and_qual (creates AND combination of qualifiers)
  - contain_aggs_of_level (checks for aggregate functions)
  - checkExprHasSubLink (detects sublinks in expressions)
- Called from (representative examples):
  - rewriteRuleAction (during rule action processing)
  - rewriteTargetView (during view rewriting)
  - AddInvertedQual (for adding inverted qualifiers)

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:1057-1124
- Returns early without modification if qual parameter is NULL
- Creates a copy of the qualifier to avoid modifying the original
- Includes safety checks to prevent invalid query structures
- Part of PostgreSQL's rule and rewriting system
- Special handling for NOTIFY statements allows rules to execute even when qualifiers would normally prevent them
- Maintains query metadata (hasSubLinks flag) to ensure proper query processing downstream
- Used extensively in rule processing where dynamic addition of WHERE conditions is common