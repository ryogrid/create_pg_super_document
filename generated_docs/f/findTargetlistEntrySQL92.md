# findTargetlistEntrySQL92

## Location
src/backend/parser/parse_clause.c: 2006 - 2171

## Overview
Returns the targetlist entry matching a given (untransformed) node, implementing SQL92-style interpretation for ORDER BY, GROUP BY, and DISTINCT ON expressions where column names or position numbers can be used.

## Definition


## Detailed Description
This function supports the old SQL92 ORDER BY interpretation where expressions can be:
1. **Column names**: Bare identifiers that match output column names in the SELECT list
2. **Position numbers**: Integer constants referring to the n'th item in the target list

The function handles two main cases:
- **Bare ColumnName**: Searches for matching column names in the existing target list. For GROUP BY, it first checks if the name matches a FROM-clause column before checking targetlist entries, adhering to SQL92 specifications.
- **IntegerConstant**: Uses the n'th item in the existing target list, with validation that the position exists.

If neither special case applies, it falls through to SQL99 rules by calling findTargetlistEntrySQL99. The function ensures that resjunk targets (internal columns not written by users) are never matched, and validates found entries using checkTargetlistEntrySQL92.

## Parameters / Member Variables
- : Parse state containing parsing context and error reporting information
- : The ORDER BY, GROUP BY, or DISTINCT ON expression to be matched
- : Pointer to the target list (passed by reference so it can be modified)
- : Enumeration identifying the clause type being processed (GROUP_BY, ORDER_BY, DISTINCT_ON)

## Dependencies
- Functions called/Symbols referenced:
  - checkTargetlistEntrySQL92
  - findTargetlistEntrySQL99
  - colNameToVar
  - equal
  - ParseExprKindName
  - intVal
  - ColumnRef
  - A_Const
  - String
  - Integer
  - EXPR_KIND_GROUP_BY
- Called from (representative examples):
  - transformGroupClauseExpr
  - transformSortClause
  - transformDistinctOnClause

## Notes and Other Information
- This is a static function within parse_clause.c for internal parser use
- Implements historical PostgreSQL behavior that extends SQL92 to allow GROUP BY with column names and position numbers
- For GROUP BY specifically, prioritizes FROM-clause column matches over targetlist matches to maintain SQL92/SQL99 compliance
- Multiple matches for the same column name are allowed only if they refer to identical expressions
- Provides detailed error messages with position information for ambiguous references and invalid position numbers
- The function bridges SQL92 and SQL99 behaviors, falling back to modern SQL99 rules when SQL92 patterns don't match